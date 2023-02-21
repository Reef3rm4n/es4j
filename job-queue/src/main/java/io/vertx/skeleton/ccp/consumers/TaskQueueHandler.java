package io.vertx.skeleton.ccp.consumers;

import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.skeleton.ccp.QueueConsumerVerticle;
import io.vertx.skeleton.ccp.QueueMessageProcessor;
import io.vertx.skeleton.ccp.mappers.MessageQueueSql;
import io.vertx.skeleton.ccp.models.QueueCircuitBreaker;
import io.vertx.skeleton.ccp.models.*;
import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.OrmIntegrityContraintViolationException;
import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.FixedDemandPacer;
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
import io.vertx.skeleton.sql.Repository;
import io.vertx.skeleton.sql.models.QueryOptions;
import io.vertx.skeleton.sql.models.RecordWithoutID;

import java.time.Duration;
import java.util.*;

import static io.vertx.skeleton.models.MessageState.*;
import static io.vertx.skeleton.models.MessageState.RETRY;
import static java.util.stream.Collectors.groupingBy;

public final class TaskQueueHandler implements MessageHandler {
  private final QueueConfiguration queueConfiguration;
  public final Repository<MessageTransactionID, MessageTransaction, MessageTransactionQuery> messageTransaction;
  public final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue;
  private final List<MessageProcessorWrapper> messageProcessors;
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskQueueHandler.class);

  public TaskQueueHandler(
    final QueueConfiguration queueConfiguration,
    final List<MessageProcessorWrapper> messageProcessorWrappers,
    final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> queue,
    final Repository<MessageTransactionID, MessageTransaction, MessageTransactionQuery> messageTransaction
  ) {
    this.messageQueue = queue;
    this.queueConfiguration = queueConfiguration;
    this.messageTransaction = messageTransaction;
    this.messageProcessors = messageProcessorWrappers;
  }

  @Override
  public Uni<Void> process(final List<MessageRecord> queueEntries) {
    return processMessages(queueEntries).flatMap(this::ackResults);
  }

  private Uni<Void> ackResults(List<MessageRecord> entries) {
    return Uni.join().all(ack(entries), requeue(entries))
      .andFailFast().replaceWithVoid();
  }

  private Uni<Void> requeue(List<MessageRecord> messages) {
    return Uni.join().all(requeueMessages(messages))
      .andFailFast().replaceWithVoid();
  }

  private Uni<Void> requeueMessages(List<MessageRecord> messages) {
    final var entriesToUpdate = messages.stream()
      .filter(entry -> entry.messageState() == MessageState.RETRY)
      .toList();
    if (!entriesToUpdate.isEmpty()) {
      LOGGER.info("re-queuing unhandled messages ->" + entriesToUpdate.stream().map(MessageRecord::id).toList());
      return messageQueue.updateByKeyBatch(entriesToUpdate);
    }
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> ack(List<MessageRecord> messages) {
    // todo move messages to dead-letter-queue
    final var groupedMessages = messages.stream()
      .filter(message -> message.messageState() == MessageState.PROCESSED ||
        message.messageState() == MessageState.FATAL_FAILURE ||
        message.messageState() == MessageState.RETRIES_EXHAUSTED ||
        message.messageState() == MessageState.EXPIRED
      )
      .collect(groupingBy(q -> q.baseRecord().tenantId()));
    final var queries = groupedMessages.entrySet().stream()
      .map(TaskQueueHandler::messageDropQuery)
      .toList();
    if (!queries.isEmpty()) {
      return Multi.createFrom().iterable(queries)
        .onItem().transformToUniAndMerge(messageQueue::deleteQuery)
        .collect().asList()
        .replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }


  private static MessageRecordQuery messageDropQuery(Map.Entry<String, List<MessageRecord>> entry) {
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
    return queueConfiguration;
  }

  public <T> QueueCircuitBreaker<T> findMessageProcessor(MessageRecord entry) {
    return customConsumers.entrySet().stream()
      .filter(t -> t.getKey().stream().anyMatch(tt -> tt.equals(entry.baseRecord().tenantId())))
      .map(Map.Entry::getValue)
      .findFirst()
      .orElse(defaultConsumers);
  }


  public Uni<List<MessageRecord>> processMessages(final List<MessageRecord> messages) {
    final List<MessageRecord> results = new ArrayList<>();
    return startStream(messages)
      .onItem().transformToUniAndMerge(message -> processMessage(results, message))
      .collect().asList()
      .replaceWithVoid();
  }

  private Uni<Void> processMessage(List<MessageRecord> messageProcessResults, MessageRecord message) {
    final var processor = findMessageProcessor(message);
    if (queueConfiguration.idempotentProcessors()) {
      return messageTransaction.transaction(
          sqlConnection -> messageProcessorTransaction(message, processor, sqlConnection)
            .flatMap(Unchecked.function(avoid -> process(message, sqlConnection)))
        )
        .onItemOrFailure().transformToUni(
          (result, throwable) -> handleProcessorResult(messageProcessResults, message, processor, result, throwable)
        );
    }
    return process(message, null)
      .onItemOrFailure().transformToUni(
        (result, throwable) -> handleProcessorResult(messageProcessResults, message, processor, result, throwable)
      );
  }

  private Uni<Void> process(MessageRecord message, SqlConnection sqlConnection) {
    try {
      return processMessage(message, sqlConnection)
        .onFailure().transform(throwable -> new MessageProcessorException(new Error("", throwable, 500)));
    } catch (Exception exception) {
      throw new MessageProcessorException(exception);
    }
  }

  private Uni<Void> processMessage(MessageRecord message, SqlConnection sqlConnection) {
    final var processor = resolveProcessor(message);
    return processor.process(parseMessage(message), sqlConnection);
  }

  private QueueMessageProcessor resolveProcessor(MessageRecord messageRecord) {
    return messageProcessors.stream()
      .filter(processor -> processor.doesMessageMatch(messageRecord))
      .findFirst()
      .map(processor -> processor.resolveProcessor(messageRecord.baseRecord().tenantId()))
      .orElseThrow();
  }

  private Object parseMessage(MessageRecord message) {
    final Class<?> tClass;
    try {
      tClass = Class.forName(message.payloadClass());
      return message.payload().mapTo(tClass);
    } catch (ClassNotFoundException e) {
      throw new MessageProcessorException(e);
    }
  }

  private Uni<MessageTransaction> messageProcessorTransaction(MessageRecord message, QueueCircuitBreaker processor, SqlConnection sqlConnection) {
    return messageTransaction.insert(
      new MessageTransaction(
        message.id(),
        processor.getDelegate().getClass().getName(),
        message.payloadClass(),
        RecordWithoutID.newRecord(message.baseRecord().tenantId())
      ),
      sqlConnection
    );
  }

  private <T> Uni<Void> handleProcessorResult(List<MessageRecord> messageProcessResults, MessageRecord message, QueueCircuitBreaker<T> messageProcessor, Throwable throwable) {
    if (throwable != null) {
      final var ne = newStateAfterFailure(queueConfiguration, message, throwable, messageProcessor);
      if (ne != null) {
        messageProcessResults.add(ne);
      }
    } else {
      LOGGER.info(messageProcessor.getDelegate().getClass().getName() + " has correctly processed the message -> " + message.id());
      final var ne = message.withState(PROCESSED);
      messageProcessResults.add(ne);
    }
    return Uni.createFrom().voidItem();
  }

  private Multi<MessageRecord> startStream(List<MessageRecord> queueEntries) {
    if (queueConfiguration.concurrency() != null) {
      final var pacer = new FixedDemandPacer(
        queueConfiguration.concurrency(),
        Duration.ofMillis(queueConfiguration.throttleInMs())
      );
      return Multi.createFrom().iterable(queueEntries)
        .paceDemand().using(pacer);
    }
    return Multi.createFrom().iterable(queueEntries);
  }

  private <T> MessageRecord newStateAfterFailure(final QueueConfiguration configuration, final MessageRecord messageRecord, Throwable throwable, QueueCircuitBreaker<T> processor) {
    if (throwable instanceof OpenCircuitException) {
      return circuitBreakerOpen(messageRecord, processor);
    } else if (throwable instanceof OrmIntegrityContraintViolationException) {
      return duplicatedMessage(messageRecord, processor);
    } else {
      return retryableFailure(configuration, messageRecord, throwable, processor);
    }
  }


  private <T> MessageRecord retryableFailure(QueueConfiguration configuration, MessageRecord messageRecord, Throwable throwable, QueueCircuitBreaker<T> processor) {
    MessageState failureState;
    if (processor.fatal().stream().anyMatch(f -> f.isAssignableFrom(throwable.getClass()))) {
      LOGGER.error("Fatal failure in processor" + processor.getDelegate().getClass().getName() + ", ccp will drop message -> " + messageRecord.id(), throwable.getCause());
      failureState = FATAL_FAILURE;
    } else if (messageRecord.retryCounter() + 1 >= configuration.maxRetry()) {
      LOGGER.error("Retries exhausted for message -> " + messageRecord.id(), throwable.getCause());
      failureState = RETRIES_EXHAUSTED;
    } else {
      LOGGER.error("Failure in processor" + processor.getDelegate().getClass().getName() + ",  ccp will requeue message for retry -> " + messageRecord.id(), throwable.getCause());
      failureState = RETRY;
    }
    return new MessageRecord(
      messageRecord.id(),
      messageRecord.scheduled(),
      messageRecord.expiration(),
      messageRecord.priority(),
      messageRecord.retryCounter() + 1,
      failureState,
      messageRecord.payloadClass(),
      messageRecord.payload(),
      new JsonObject().put(processor.getDelegate().getClass().getName(), throwable.getMessage()),
      messageRecord.verticleId(),
      messageRecord.baseRecord()
    );
  }


  private <T> MessageRecord duplicatedMessage(MessageRecord messageRecord, QueueCircuitBreaker<T> processor) {
    LOGGER.warn(processor.getDelegate().getClass().getName() + " has already processed message " + messageRecord.id() + ", message will be dropped from queue");
    return null;
  }


  private <T> MessageRecord circuitBreakerOpen(MessageRecord messageRecord, QueueCircuitBreaker<T> processor) {
    LOGGER.error("Circuit-Breaker open for processor " + processor.getDelegate().getClass().getName() + ", ccp will requeue message for retry -> " + messageRecord.id());
    return new MessageRecord(
      messageRecord.id(),
      messageRecord.scheduled(),
      messageRecord.expiration(),
      messageRecord.priority(),
      messageRecord.retryCounter(),
      RETRY,
      messageRecord.payloadClass(),
      messageRecord.payload(),
      new JsonObject().put(processor.getDelegate().getClass().getName(), "circuit breaker open"),
      messageRecord.verticleId(),
      messageRecord.baseRecord()
    );
  }

  public QueueConfiguration configuration() {
    return queueConfiguration;
  }

  public Logger logger() {
    return LOGGER;
  }


// todo re-implement this
//  private <T> QueueCircuitBreaker<T> wrapCircuitBreaker(QueueMessageProcessor<T> processor, PgSubscriber pgSubscriber, Vertx vertx, Logger logger, QueueConfiguration configuration) {
//    final var circuitName = MessageQueueSql.camelToSnake(processor.getClass().getSimpleName() + "_circuit_breaker");
//    final var circuitBreaker = CircuitBreaker.create(circuitName, vertx,
//      new CircuitBreakerOptions()
//        .setMaxFailures(configuration.circuitBreakerMaxFailues())
//    );
//    circuitBreaker
//      .retryPolicy(RetryPolicy.constantDelay(configuration.retryIntervalInMinutes() * 60000L))
//      .openHandler(() -> openCircuits.add(processor.getClass()))
//      .closeHandler(() -> openCircuits.remove(processor.getClass()));
//    return new QueueCircuitBreaker<>(processor, circuitBreaker);
//  }

}
