package io.es4j.queue.postgres;


import io.es4j.queue.postgres.mappers.DeadLetterMapper;
import io.es4j.queue.postgres.mappers.MessageQueueMapper;
import io.es4j.queue.postgres.mappers.PgQueueLiquibase;
import io.es4j.queue.postgres.models.*;
import io.es4j.sql.Repository;
import io.es4j.sql.RepositoryHandler;
import io.es4j.sql.exceptions.NotFound;
import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.QueryOptions;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.FixedDemandPacer;
import io.es4j.queue.models.MessageState;
import io.es4j.queue.models.QueueConfiguration;
import io.es4j.queue.models.RawMessage;
import io.es4j.queue.models.MessageProcessorManager;
import io.smallrye.mutiny.Uni;

import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.es4j.queue.TaskSubscriber;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.stream.Collectors.groupingBy;

public class PgTaskSubscriber implements TaskSubscriber {

  public static final AtomicBoolean LIQUIBASE_DEPLOYED = new AtomicBoolean(false);
  private final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue;
  private final Repository<DeadLetterKey, DeadLetterRecord, MessageRecordQuery> deadLetterQueue;
  private final io.vertx.mutiny.pgclient.pubsub.PgSubscriber pgSubscriber;
  private static final Logger LOGGER = LoggerFactory.getLogger(PgTaskSubscriber.class);

  public PgTaskSubscriber(Vertx vertx, JsonObject configuration) {
    final var repositoryHandler = RepositoryHandler.leasePool(configuration, vertx);
    this.messageQueue = new Repository<>(MessageQueueMapper.INSTANCE, repositoryHandler);
    this.deadLetterQueue = new Repository<>(DeadLetterMapper.INSTANCE, repositoryHandler);
    this.pgSubscriber = io.vertx.mutiny.pgclient.pubsub.PgSubscriber.subscriber(
      repositoryHandler.vertx(),
      RepositoryHandler.connectionOptions(repositoryHandler.configuration())
    );
    pgSubscriber.reconnectPolicy(integer -> 0L);
  }


  @Override
  public Uni<Void> unsubscribe() {
    return pgSubscriber.close();
  }

  @Override
  public Uni<Void> subscribe(MessageProcessorManager messageProcessorManager) {
    // todo put schema in channel
    final var pgChannel = pgSubscriber.channel("task_queue_ch");
    pgChannel.handler(payload -> {
          pgChannel.pause();
          LOGGER.info("Message available {}", payload);
          poll(messageProcessorManager, null)
            .subscribe()
            .with(
              item -> {
                LOGGER.info("Queue empty, resuming subscription");
                pgChannel.resume();
              },
              throwable -> {
                if (throwable instanceof NoStackTraceThrowable illegalStateException) {
                  LOGGER.info(illegalStateException.getMessage());
                } else if (throwable instanceof NotFound) {
                  LOGGER.info("Queue empty");
                } else {
                  LOGGER.error("PgSubscriber dropped exception", throwable);
                }
                pgChannel.resume();
              }
            );
        }
      )
      .endHandler(() -> LOGGER.info("pg-channel subscription stopped"))
      .subscribeHandler(() -> LOGGER.info("subscribed to pg-channel"))
      .exceptionHandler(throwable -> LOGGER.error("Error in pg-subscription", throwable));
    return PgQueueLiquibase.bootstrapQueue(messageQueue.repositoryHandler(), messageProcessorManager.queueConfiguration())
      .flatMap(avoid -> pgSubscriber.connect());
  }

  private Multi<MessageRecord> startPacedStream(QueueConfiguration queueConfiguration, List<MessageRecord> messageRecords) {
    if (queueConfiguration.concurrency() != null) {
      final var pacer = new FixedDemandPacer(
        queueConfiguration.concurrency(),
        Duration.ofMillis(queueConfiguration.throttleInMs())
      );
      return Multi.createFrom().iterable(messageRecords)
        .paceDemand().using(pacer);
    }
    return Multi.createFrom().iterable(messageRecords);
  }

  private Uni<Void> poll(MessageProcessorManager taskManager, String verticleId) {
    return pollBatch(taskManager.queueConfiguration(), verticleId)
      .onItem().transformToMulti(messageRecords -> startPacedStream(taskManager.queueConfiguration(), messageRecords))
      .onItem().transformToUniAndMerge(messageRecord -> taskManager.processMessage(parseRecord(messageRecord)))
      .collect().asList()
      .flatMap(rawMessages -> handleResults(rawMessages.stream().map(MessageRecord::from).toList()))
      .replaceWithVoid()
      .flatMap(avoid -> poll(taskManager, verticleId));
  }

  private RawMessage parseRecord(MessageRecord messageRecord) {
    return new RawMessage(
      messageRecord.id(),
      messageRecord.scheduled(),
      messageRecord.expiration(),
      messageRecord.priority(),
      messageRecord.retryCounter(),
      messageRecord.messageState(),
      messageRecord.payloadClass(),
      messageRecord.payload(),
      messageRecord.failedProcessors(),
      messageRecord.baseRecord().tenant()
    );
  }

  private Uni<List<MessageRecord>> pollBatch(QueueConfiguration configuration, String deploymentId) {
    return messageQueue.query(pollingStatement(configuration, deploymentId)).onFailure(NotFound.class)
      .recoverWithUni(
        () -> messageQueue.query(recoveryPollingStatement(configuration, deploymentId))
          .map(messageRecords -> messageRecords.stream().map(m -> m.withState(MessageState.RECOVERY)).toList())
      );
  }

  private String pollingStatement(
    final QueueConfiguration configuration,
    String deploymentId
  ) {
    return "update task_queue set state = 'PROCESSING', verticle_id = '" + deploymentId + "' where message_id in (" +
      " select message_id from task_queue where " +
      " state in ('CREATED','SCHEDULED','RETRY')" +
      " and (scheduled is null or scheduled <= current_timestamp)" +
      " and (expiration is null or expiration >= current_timestamp)" +
      " and (retry_counter = 0 or updated + interval '" + configuration.retryIntervalInSeconds() + " seconds' <= current_timestamp)" +
      " order by priority for update skip locked limit " + configuration.batchSize() +
      " ) returning *;";
  }

  private String recoveryPollingStatement(
    final QueueConfiguration configuration,
    final String deploymentId
  ) {
    return "update task_queue set state = 'PROCESSING', verticle_id = '" + deploymentId + "' where message_id in (" +
      " select message_id from task_queue where " +
      " state = 'RECOVERY' " +
      " order by priority for update skip locked limit " + configuration.batchSize() +
      " ) returning *;";
  }

  private Uni<Void> handleResults(List<MessageRecord> messages) {
    return Uni.join().all(ack(messages), nack(messages)).andFailFast().replaceWithVoid();
  }

  private Uni<Void> nack(List<MessageRecord> messages) {
    return Uni.join().all(requeueMessages(messages)).andFailFast().replaceWithVoid();
  }

  private Uni<Void> requeueMessages(List<MessageRecord> messages) {
    final var messagesToRequeue = messages.stream()
      .filter(entry -> entry.messageState() == MessageState.RETRY)
      .toList();
    if (!messagesToRequeue.isEmpty()) {
      LOGGER.info("re-queuing unhandled messages ->" + messagesToRequeue.stream().map(MessageRecord::id).toList());
      return messageQueue.updateByKeyBatch(messagesToRequeue);
    }
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> ack(List<MessageRecord> messages) {
    // todo move messages to dead-letter-queue
    final var messagesToAckOrNack = messages.stream()
      .filter(message -> message.messageState() == MessageState.PROCESSED ||
        message.messageState() == MessageState.FATAL_FAILURE ||
        message.messageState() == MessageState.RETRIES_EXHAUSTED ||
        message.messageState() == MessageState.EXPIRED
      )
      .collect(groupingBy(q -> q.baseRecord().tenant()));
    final var queries = messagesToAckOrNack.entrySet().stream()
      .map(this::messageDropQuery)
      .toList();
    final var deadLetters = messages.stream()
      .filter(message ->
        message.messageState() == MessageState.FATAL_FAILURE ||
          message.messageState() == MessageState.RETRIES_EXHAUSTED || message.messageState() == MessageState.EXPIRED
      )
      .map(messageRecord -> new DeadLetterRecord(
        messageRecord.id(),
        messageRecord.scheduled(),
        messageRecord.expiration(),
        messageRecord.priority(),
        messageRecord.retryCounter(),
        messageRecord.messageState(),
        messageRecord.payloadClass(),
        messageRecord.payload(),
        messageRecord.failedProcessors(),
        messageRecord.verticleId(),
        BaseRecord.newRecord(messageRecord.baseRecord().tenant())
      ))
      .toList();
    if (!queries.isEmpty()) {
      return Multi.createFrom().iterable(queries)
        .onItem().transformToUniAndMerge(messageQueue::deleteQuery)
        .collect().asList()
        .flatMap(avoid -> deadLetters.isEmpty() ? Uni.createFrom().voidItem() : deadLetterQueue.insertBatch(deadLetters))
        .replaceWithVoid();
    }
    return deadLetters.isEmpty() ? Uni.createFrom().voidItem() : deadLetterQueue.insertBatch(deadLetters);
  }


  private MessageRecordQuery messageDropQuery(Map.Entry<String, List<MessageRecord>> entry) {
    return MessageRecordQueryBuilder.builder()
      .ids(entry.getValue().stream().map(MessageRecord::id).toList())
      .options(QueryOptions.simple(entry.getKey()))
      .build();
  }

}
