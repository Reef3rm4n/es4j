package io.vertx.skeleton.ccp.consumers;

import io.vertx.skeleton.ccp.SingleProcessConsumer;
import io.vertx.skeleton.ccp.mappers.MessageQueueSql;
import io.vertx.skeleton.ccp.models.SingleWithCircuitBreaker;
import io.vertx.skeleton.ccp.models.*;
import io.vertx.skeleton.orm.Repository;
import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.FixedDemandPacer;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.circuitbreaker.OpenCircuitException;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.circuitbreaker.CircuitBreaker;
import io.vertx.mutiny.circuitbreaker.RetryPolicy;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.pgclient.pubsub.PgSubscriber;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.skeleton.models.*;

import java.time.Duration;
import java.util.*;

import static io.vertx.skeleton.models.MessageState.*;
import static io.vertx.skeleton.models.MessageState.RETRY;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

public final class SingleProcessHandler<T, R> implements MessageConsumer {
  private final String deploymentId;
  private final Set<Class<?>> openCircuits = new HashSet<>();
  private final SingleWithCircuitBreaker<T, R> defaultConsumers;
  private final Map<List<Tenant>, SingleWithCircuitBreaker<T, R>> customConsumers;
  private final Class<T> payloadClass;
  private final QueueConfiguration configuration;
  private final Logger logger;
  public final Repository<MessageTransactionID, MessageTransaction, MessageTransactionQuery> transactionLog;
  public final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue;
  private final ProcessEventConsumerHandler<T, R> processEventConsumer;

  public SingleProcessHandler(
    final SingleWrapper<T, R> singleWrapper,
    final PgSubscriber pgSubscriber,
    final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> queue,
    final Repository<MessageTransactionID, MessageTransaction, MessageTransactionQuery> transactionLog
  ) {
    this.messageQueue = queue;
    this.transactionLog = transactionLog;
    this.processEventConsumer = new ProcessEventConsumerHandler<>(singleWrapper.logger(), singleWrapper.payloadClass(), singleWrapper.consumers());
    this.deploymentId = singleWrapper.deploymentId();
    this.payloadClass = singleWrapper.payloadClass();
    this.configuration = singleWrapper.configuration();
    this.logger = singleWrapper.logger();
    this.customConsumers = singleWrapper.customProcessors().entrySet().stream()
      .map(entry -> Map.entry(entry.getKey(), wrapCircuitBreaker(entry.getValue(), pgSubscriber, transactionLog.repositoryHandler().vertx(), logger, configuration)))
      .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    this.defaultConsumers = wrapCircuitBreaker(singleWrapper.defaultProcessor(), pgSubscriber, transactionLog.repositoryHandler().vertx(), logger, configuration);
  }

  @Override
  public Uni<Void> process(final List<MessageRecord> queueEntries) {
    return processMessages(queueEntries)
      .call(tuple -> ackResults(tuple.getItem2()))
      .flatMap(tuple -> processEventConsumer.consumeEvents(tuple.getItem1())
        .onFailure().recoverWithNull()
        .onItem().transformToUni(avoid -> {
            final var failures = tuple.getItem1().stream()
              .map(QueueEvent::throwable)
              .filter(Objects::nonNull)
              .toList();
            if (!failures.isEmpty()) {
              return Uni.createFrom().failure(new CompositeException(failures));
            }
            return Uni.createFrom().voidItem();
          }
        )
      );
  }

  private Uni<Void> ackResults(List<MessageRecord> entries) {
    return Uni.join().all(ack(entries), requeue(entries))
      .andFailFast().replaceWithVoid();
  }

  private Uni<Void> requeue(List<MessageRecord> messages) {
    return Uni.join().all(requeueMessages(messages), enqueueMessages(messages))
      .andFailFast().replaceWithVoid();
  }

  private Uni<Void> requeueMessages(List<MessageRecord> messages) {
    final var entriesToUpdate = messages.stream()
      .filter(entry -> entry.persistedRecord().id() != null)
      .filter(entry -> entry.messageState() == MessageState.RETRY)
      .toList();
    if (!entriesToUpdate.isEmpty()) {
      logger.info("re-queuing unhandled messages ->" + entriesToUpdate.stream().map(MessageRecord::id).toList());
      return messageQueue.updateByKeyBatch(entriesToUpdate);
    }
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> enqueueMessages(List<MessageRecord> messages) {
    final var entriesToInsert = messages.stream()
      .filter(entry -> entry.persistedRecord().id() == null)
      .filter(entry -> entry.messageState() == MessageState.RETRY)
      .toList();
    if (!entriesToInsert.isEmpty()) {
      logger.info("queueing unhandled messages ->" + entriesToInsert.stream().map(MessageRecord::id).toList());
      return messageQueue.insertBatch(entriesToInsert);
    }
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> ack(List<MessageRecord> messages) {
    final var groupedMessages = messages.stream()
      .filter(entry -> Objects.nonNull(entry.persistedRecord().id())) // message never passed through the queue thus there's nothing to ack on
      .filter(message -> message.messageState() == MessageState.PROCESSED ||
        message.messageState() == MessageState.FATAL_FAILURE ||
        message.messageState() == MessageState.RETRIES_EXHAUSTED ||
        message.messageState() == MessageState.EXPIRED
      )
      .collect(groupingBy(q -> q.persistedRecord().tenant()));
    final var queries = groupedMessages.entrySet().stream()
      .map(SingleProcessHandler::messageDropQuery)
      .toList();
    if (!queries.isEmpty()) {
      logger.info("Ack messages");
      return Multi.createFrom().iterable(queries)
        .onItem().transformToUniAndMerge(messageQueue::deleteQuery)
        .collect().asList()
        .replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }


  private static MessageRecordQuery messageDropQuery(Map.Entry<Tenant, List<MessageRecord>> entry) {
    return new MessageRecordQuery(
      entry.getValue().stream().map(MessageRecord::id).toList(),
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      QueryOptions.simple(entry.getKey())
    );
  }

  @Override
  public QueueConfiguration queueConfiguration() {
    return configuration;
  }

  public SingleWithCircuitBreaker<T, R> findMessageProcessor(MessageRecord entry) {
    return customConsumers.entrySet().stream()
      .filter(t -> t.getKey().stream().anyMatch(tt -> tt.equals(entry.persistedRecord().tenant())))
      .map(Map.Entry::getValue)
      .findFirst()
      .orElse(defaultConsumers);
  }


  public Uni<Tuple2<List<QueueEvent<T, R>>, List<MessageRecord>>> processMessages(final List<MessageRecord> messages) {
    final List<MessageRecord> results = new ArrayList<>();
    final List<QueueEvent<T, R>> events = new ArrayList<>();
    return startStream(messages)
      .onItem().transformToUniAndMerge(message -> processMessage(results, events, message))
      .collect().asList()
      .map(avoid -> Tuple2.of(events, results));
  }

  private Uni<Void> processMessage(List<MessageRecord> messageProcessResults, List<QueueEvent<T, R>> messageProcessEvents, MessageRecord message) {
    final var processor = findMessageProcessor(message);
    if (configuration.idempotentProcessors()) {
      return transactionLog.transaction(
          sqlConnection -> messageProcessorTransaction(message, processor, sqlConnection)
            .flatMap(Unchecked.function(avoid -> process(message, processor, sqlConnection)))
        )
        .onItemOrFailure().transformToUni(
          (result, throwable) -> handleProcessorResult(messageProcessResults, messageProcessEvents, message, processor, result, throwable)
        );
    }
    return process(message, processor, null)
      .onItemOrFailure().transformToUni(
        (result, throwable) -> handleProcessorResult(messageProcessResults, messageProcessEvents, message, processor, result, throwable)
      );
  }

  private Uni<R> process(MessageRecord message, SingleWithCircuitBreaker<T, R> processor, SqlConnection sqlConnection) {
    try {
      return processor.process(message.payload().mapTo(payloadClass), sqlConnection)
        .onFailure().transform(MessageProcessorException::new);
    } catch (Exception exception) {
      throw new MessageProcessorException(exception);
    }
  }

  private Uni<MessageTransaction> messageProcessorTransaction(MessageRecord message, SingleWithCircuitBreaker<T, R> processor, SqlConnection sqlConnection) {
    return transactionLog.insert(
      new MessageTransaction(
        message.id(),
        processor.getDelegate().getClass().getName(),
        ProcessorType.SINGLE,
        PersistedRecord.newRecord(message.persistedRecord().tenant())
      ),
      sqlConnection
    );
  }

  private Uni<Void> handleProcessorResult(List<MessageRecord> messageProcessResults, List<QueueEvent<T, R>> messageProcessEvents, MessageRecord message, SingleWithCircuitBreaker<T, R> messageProcessor, R messageResult, Throwable throwable) {
    if (throwable != null) {
      final var ne = newStateAfterFailure(configuration, message, throwable, messageProcessor);
      if (ne != null) {
        messageProcessResults.add(ne);
        messageProcessEvents.add(parseEvent(ne, null, throwable));
      }
    } else {
      logger.info(messageProcessor.getDelegate().getClass().getName() + " has correctly processed the message -> " + message.id());
      final var ne = message.withState(PROCESSED);
      messageProcessResults.add(ne);
      messageProcessEvents.add(parseEvent(ne, messageResult, null));
    }
    return Uni.createFrom().voidItem();
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

  private QueueEvent<T, R> parseEvent(MessageRecord messageRecord, R result, Throwable throwable) {
    return switch (messageRecord.messageState()) {
      case EXPIRED -> QueueEvent.of(
        messageRecord.id(),
        messageRecord.persistedRecord().tenant(),
        messageRecord.payload().mapTo(payloadClass()),
        result,
        throwable,
        QueueEventType.EXPIRED,
        messageRecord.persistedRecord().version()
      );
      case RETRY -> QueueEvent.of(
        messageRecord.id(),
        messageRecord.persistedRecord().tenant(),
        messageRecord.payload().mapTo(payloadClass()),
        result,
        throwable,
        QueueEventType.FAILURE,
        messageRecord.persistedRecord().version()
      );
      case RETRIES_EXHAUSTED -> QueueEvent.of(
        messageRecord.id(),
        messageRecord.persistedRecord().tenant(),
        messageRecord.payload().mapTo(payloadClass()),
        result,
        throwable,
        QueueEventType.RETRIES_EXHAUSTED,
        messageRecord.persistedRecord().version()
      );
      case PROCESSED -> QueueEvent.of(
        messageRecord.id(),
        messageRecord.persistedRecord().tenant(),
        messageRecord.payload().mapTo(payloadClass()),
        result,
        null,
        QueueEventType.PROCESSED,
        messageRecord.persistedRecord().version()
      );
      case FATAL_FAILURE -> QueueEvent.of(
        messageRecord.id(),
        messageRecord.persistedRecord().tenant(),
        messageRecord.payload().mapTo(payloadClass()),
        result,
        throwable,
        QueueEventType.FATAL_FAILURE,
        messageRecord.persistedRecord().version()
      );
      default ->
        throw new IllegalStateException("Invalid state in event parser -> " + messageRecord.messageState().name());
    };
  }

  private MessageRecord newStateAfterFailure(final QueueConfiguration configuration, final MessageRecord messageRecord, Throwable throwable, SingleWithCircuitBreaker<T, R> processor) {
    if (throwable instanceof OpenCircuitException) {
      return circuitBreakerOpen(messageRecord, processor);
    } else if (throwable instanceof OrmIntegrityContraintViolationException) {
      return duplicatedMessage(messageRecord, processor);
    } else {
      return retryableFailure(configuration, messageRecord, throwable, processor);
    }
  }


  private MessageRecord retryableFailure(QueueConfiguration configuration, MessageRecord messageRecord, Throwable throwable, SingleWithCircuitBreaker<T, R> processor) {
    MessageState failureState;
    if (processor.fatal().stream().anyMatch(f -> f.isAssignableFrom(throwable.getClass()))) {
      logger.error("Fatal failure in processor" + processor.getDelegate().getClass().getName() + ", ccp will drop message -> " + messageRecord.id(), throwable.getCause());
      failureState = FATAL_FAILURE;
    } else if (messageRecord.retryCounter() + 1 >= configuration.maxRetry()) {
      logger.error("Retries exhausted for message -> " + messageRecord.id(), throwable.getCause());
      failureState = RETRIES_EXHAUSTED;
    } else {
      logger.error("Failure in processor" + processor.getDelegate().getClass().getName() + ",  ccp will requeue message for retry -> " + messageRecord.id(), throwable.getCause());
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
      new JsonObject().put(processor.getDelegate().getClass().getName(), throwable.getMessage()),
      messageRecord.verticleId(),
      messageRecord.persistedRecord()
    );
  }


  private MessageRecord duplicatedMessage(MessageRecord messageRecord, SingleWithCircuitBreaker<T, R> processor) {
    logger.warn(processor.getDelegate().getClass().getName() + " has already processed message " + messageRecord.id() + ", message will be dropped from queue");
    return null;
  }


  private MessageRecord circuitBreakerOpen(MessageRecord messageRecord, SingleWithCircuitBreaker<T, R> processor) {
    logger.error("Circuit-Breaker open for processor " + processor.getDelegate().getClass().getName() + ", ccp will requeue message for retry -> " + messageRecord.id());
    return new MessageRecord(
      messageRecord.id(),
      messageRecord.scheduled(),
      messageRecord.expiration(),
      messageRecord.priority(),
      messageRecord.retryCounter(),
      RETRY,
      messageRecord.payload(),
      new JsonObject().put(processor.getDelegate().getClass().getName(), "circuit breaker open"),
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

  private SingleWithCircuitBreaker<T, R> wrapCircuitBreaker(SingleProcessConsumer<T, R> processor, PgSubscriber pgSubscriber, Vertx vertx, Logger logger, QueueConfiguration configuration) {
    final var circuitName = MessageQueueSql.camelToSnake(processor.getClass().getSimpleName() + "_circuit_breaker");
    final var circuitBreaker = CircuitBreaker.create(circuitName, vertx,
      new CircuitBreakerOptions()
        .setMaxFailures(configuration.circuitBreakerMaxFailues())
    );
    circuitBreaker
      .retryPolicy(RetryPolicy.constantDelay(configuration.retryIntervalInMinutes() * 60000L))
      .openHandler(() -> openCircuits.add(processor.getClass()))
      .closeHandler(() -> openCircuits.remove(processor.getClass()));
    return new SingleWithCircuitBreaker<>(processor, circuitBreaker);
  }

}
