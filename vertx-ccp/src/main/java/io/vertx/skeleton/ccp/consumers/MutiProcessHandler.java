package io.vertx.skeleton.ccp.consumers;

import io.vertx.skeleton.ccp.MultiProcessConsumer;
import io.vertx.skeleton.ccp.mappers.MessageQueueSql;
import io.vertx.skeleton.ccp.models.MultiWrapper;
import io.vertx.skeleton.ccp.models.*;
import io.vertx.skeleton.orm.Repository;
import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.FixedDemandPacer;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.circuitbreaker.OpenCircuitException;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.circuitbreaker.CircuitBreaker;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.skeleton.models.MessageState;
import io.vertx.skeleton.models.PersistedRecord;
import io.vertx.skeleton.models.Tenant;

import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static io.vertx.skeleton.models.MessageState.*;
import static io.vertx.skeleton.models.MessageState.RETRY;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

public final class MutiProcessHandler<T> implements MessageConsumer {
  private final String deploymentId;
  private final List<MultiWithCircuitBreaker<T>> defaultProcessor;
  private final Map<List<Tenant>, MultiWithCircuitBreaker<T>> customProcessors;
  private final Class<T> payloadClass;
  private final QueueConfiguration configuration;
  private final Logger logger;
  public final Repository<MessageTransactionID, MessageTransaction, MessageTransactionQuery> transactionLog;
  public final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue;


  public MutiProcessHandler(
    final MultiWrapper<T> multiWrapper,
    final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> queue,
    final Repository<MessageTransactionID, MessageTransaction, MessageTransactionQuery> trasactionLog
  ) {
    this.deploymentId = multiWrapper.deploymentId();
    this.payloadClass = multiWrapper.payloadClass();
    this.configuration = multiWrapper.configuration();
    this.transactionLog = trasactionLog;
    this.messageQueue = queue;
    this.logger = multiWrapper.logger();
    this.customProcessors = multiWrapper.customProcessors().entrySet().stream()
      .map(entry -> Map.entry(entry.getKey(), wrapCircuitBreaker(entry.getValue(), trasactionLog.repositoryHandler().vertx())))
      .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    this.defaultProcessor = multiWrapper.customProcessors().values().stream()
      .map(tMultiProcessConsumer -> wrapCircuitBreaker(tMultiProcessConsumer, trasactionLog.repositoryHandler().vertx()))
      .toList();
  }

  public Uni<Void> process(final List<MessageRecord> queueEntries) {
    return processEntries(queueEntries)
      .flatMap(this::ack);
  }


  private Uni<Void> ack(List<MessageRecord> messages) {
    return messageQueue.transaction(
      sqlConnection -> Uni.join().all(ack(messages, sqlConnection), requeue(messages, sqlConnection))
        .andFailFast()
        .replaceWithVoid()
    );
  }

  public Uni<Void> requeue(List<MessageRecord> messages, SqlConnection sqlConnection) {
    final var messagesToRequeue = messages.stream()
      .filter(entry -> entry.messageState() == MessageState.RETRY)
      .toList();
    if (!messagesToRequeue.isEmpty()) {
      return messageQueue.updateByKeyBatch(messagesToRequeue, sqlConnection);
    }
    return Uni.createFrom().voidItem();
  }

  public Uni<Void> ack(List<MessageRecord> messages, SqlConnection sqlConnection) {
    final var messagesToDelete = messages.stream()
      .filter(entry -> entry.messageState() == MessageState.PROCESSED || entry.messageState() == MessageState.FATAL_FAILURE || entry.messageState() == MessageState.RETRIES_EXHAUSTED || entry.messageState() == MessageState.EXPIRED)
      .map(MessageRecord::persistedRecord).map(PersistedRecord::id).toList();
    if (!messagesToDelete.isEmpty()) {
      return messageQueue.deleteByIdBatch(messagesToDelete, sqlConnection);
    }
    return Uni.createFrom().voidItem();
  }

  @Override
  public QueueConfiguration queueConfiguration() {
    return configuration;
  }

  public List<MultiWithCircuitBreaker<T>> findProcessors(MessageRecord entry) {
    if (entry.failedProcessors() != null && !entry.failedProcessors().isEmpty()) {
      // filters out only processors that failed
      final var customConsumers = customProcessors.entrySet().stream()
        .filter(t -> t.getKey().stream().anyMatch(tt -> tt.equals(entry.persistedRecord().tenant())))
        .filter(t -> entry.failedProcessors().stream().anyMatch(f -> f.getKey().equals(t.getValue().getDelegate().getClass().getName())))
        .map(Map.Entry::getValue);
      return Stream.concat(
          customConsumers,
          defaultProcessor.stream()
            .filter(t -> entry.failedProcessors().stream().anyMatch(f -> f.getKey().equals(t.getDelegate().getClass().getName())))
        )
        .toList();
    } else {
      final var customProcessors = this.customProcessors.entrySet().stream()
        .filter(t -> t.getKey().stream().anyMatch(tt -> tt.equals(entry.persistedRecord().tenant())))
        .map(Map.Entry::getValue);
      return Stream.concat(customProcessors, defaultProcessor.stream()).toList();
    }
  }

  private MultiWithCircuitBreaker<T> wrapCircuitBreaker(MultiProcessConsumer<T> processor, Vertx vertx) {
    final var circuitBreakerName = MessageQueueSql.camelToSnake(processor.getClass().getSimpleName() + "_circuit_breaker");
    final var circuitBreaker = CircuitBreaker.create(circuitBreakerName, vertx,
      new CircuitBreakerOptions()
        .setMaxFailures(10)
    );
    return new MultiWithCircuitBreaker<>(processor, circuitBreaker);
  }


  private Uni<List<MessageRecord>> processEntries(final List<MessageRecord> queueEntries) {
    return startStream(queueEntries)
      .onItem().transformToUniAndMerge(
        message -> {
          final var processors = findProcessors(message);
          final var failures = new HashMap<String, Throwable>();
          return Multi.createFrom().iterable(processors)
            .onItem().transformToUniAndMerge(
              processor -> {
                if (processor.configuration().idempotentProcessors()) {
                  return transactionLog.transaction(
                      sqlConnection -> messageProcessorTransaction(message, processor, sqlConnection)
                        .flatMap(avoid -> process(message, processor))
                    )
                    .onItemOrFailure().transform(
                      (result, throwable) -> handleProcessorResult(processor, throwable, failures)
                    );
                }
                return process(message, processor)
                  .onItemOrFailure().transform(
                    (result, throwable) -> handleProcessorResult(processor, throwable, failures)
                  );
              }
            )
            .collect().asList()
            .map(avoid -> {
                if (!failures.isEmpty()) {
                  return newStateAfterFailure(configuration, message, failures);
                } else {
                  return message.withState(PROCESSED);
                }
              }
            );
        }
      )
      .collect().asList();
  }

  private boolean handleProcessorResult(MultiWithCircuitBreaker<T> processor, Throwable throwable, HashMap<String, Throwable> failures) {
    if (throwable != null) {
      failures.put(processor.getDelegate().getClass().getName(), throwable);
    }
    return false;
  }

  private Uni<Void> process(MessageRecord message, MultiWithCircuitBreaker<T> processor) {
    try {
      return processor.process(message.payload().mapTo(payloadClass))
        .onFailure().transform(MessageProcessorException::new);
    } catch (Exception exception) {
      throw new MessageProcessorException(exception);
    }
  }

  private Uni<MessageTransaction> messageProcessorTransaction(MessageRecord message, MultiWithCircuitBreaker<T> processor, SqlConnection sqlConnection) {
    return transactionLog.insert(
      new MessageTransaction(
        message.id(),
        processor.getDelegate().getClass().getName(),
        ProcessorType.MULTI,
        PersistedRecord.newRecord(message.persistedRecord().tenant())
      ),
      sqlConnection
    );
  }

  private Multi<MessageRecord> startStream(List<MessageRecord> queueEntries) {
    if (configuration.concurrency() != null) {
      final var pacer = new FixedDemandPacer(
        configuration.concurrency(),
        Duration.ofMillis(configuration.throttleInMs())
      );
      return Multi.createFrom().iterable(queueEntries)
        .paceDemand().using(pacer);
    }
    return Multi.createFrom().iterable(queueEntries);
  }

  private MessageRecord newStateAfterFailure(final QueueConfiguration configuration, final MessageRecord messageRecord, Map<String, Throwable> failures) {
    if (failures.values().stream().anyMatch(throwable -> throwable instanceof OpenCircuitException)) {
      return circuitBreakerOpen(messageRecord, failures);
    } else {
      return retryableFailure(configuration, messageRecord, failures);
    }
  }


  private MessageRecord retryableFailure(QueueConfiguration configuration, MessageRecord messageRecord, Map<String, Throwable> failures) {
    MessageState failureState;
    if (messageRecord.retryCounter() + 1 >= configuration.maxRetry()) {
      logger.error("Retries exhausted for message -> " + messageRecord.id(), new CompositeException(failures.values().stream().toList()));
      failureState = RETRIES_EXHAUSTED;
    } else {
      logger.error("Failure in processor ccp will requeue message for retry -> " + messageRecord.id(), new CompositeException(failures.values().stream().toList()));
      failureState = RETRY;
    }
    return new MessageRecord(
      messageRecord.id(),
      messageRecord.scheduled(),
      messageRecord.expiration(),
      messageRecord.priority(),
      messageRecord.retryCounter() + 1,
      failureState,
      messageRecord.payload(),
      JsonObject.mapFrom(failures),
      messageRecord.verticleId(),
      messageRecord.persistedRecord()
    );
  }


  private MessageRecord circuitBreakerOpen(MessageRecord messageRecord, Map<String, Throwable> failures) {
    logger.error("Circuit-Breaker open for processor , ccp will requeue message for retry -> " + messageRecord.id());
    return new MessageRecord(
      messageRecord.id(),
      messageRecord.scheduled(),
      messageRecord.expiration(),
      messageRecord.priority(),
      messageRecord.retryCounter(),
      RETRY,
      messageRecord.payload(),
      JsonObject.mapFrom(failures),
      messageRecord.verticleId(),
      messageRecord.persistedRecord()
    );
  }

  public String deploymentId() {
    return deploymentId;
  }

  public Class<T> payloadClass() {
    return payloadClass;
  }

  public QueueConfiguration configuration() {
    return configuration;
  }

  public Logger logger() {
    return logger;
  }

}
