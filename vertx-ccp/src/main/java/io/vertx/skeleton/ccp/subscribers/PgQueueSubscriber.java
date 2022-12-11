package io.vertx.skeleton.ccp.subscribers;

import io.vertx.skeleton.ccp.consumers.MessageConsumer;
import io.vertx.skeleton.ccp.models.QueueConfiguration;
import io.vertx.skeleton.ccp.models.MessageRecord;
import io.vertx.skeleton.ccp.models.MessageRecordID;
import io.vertx.skeleton.ccp.models.MessageRecordQuery;
import io.vertx.skeleton.orm.Repository;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.skeleton.models.MessageState;
import io.vertx.skeleton.models.OrmNotFoundException;

import java.util.*;

public class PgQueueSubscriber implements QueueSubscriber {
  private final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> queue;
  private final io.vertx.mutiny.pgclient.pubsub.PgSubscriber pgSubscriber;

  public PgQueueSubscriber(
    final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> concurrentQueueRepository,
    final io.vertx.mutiny.pgclient.pubsub.PgSubscriber pgSubscriber
  ) {
    this.queue = concurrentQueueRepository;
    this.pgSubscriber = pgSubscriber;
  }

  @Override
  public Uni<Void> unsubscribe() {
    return pgSubscriber.close();
  }

  @Override
  public void subscribe(MessageConsumer messageConsumer, String verticleId) {
    final var logger = LoggerFactory.getLogger(messageConsumer.queueConfiguration().queueName());
    subscribeToPgChannel(messageConsumer, verticleId, logger);
  }

  private void subscribeToPgChannel(MessageConsumer messageConsumer, String verticleId, Logger logger) {
    final var pgChannel = pgSubscriber.channel(messageConsumer.queueConfiguration().queueName() + "_ch");
    pgChannel.handler(payload -> {
          pgChannel.pause();
          logger.info("Message available !");
          pollUntilQueueIsEmpty(messageConsumer, verticleId)
            .subscribe()
            .with(
              item -> {
                logger.info("Queue empty, resuming subscription");
                pgChannel.resume();
              },
              throwable -> {
                if (throwable instanceof NoStackTraceThrowable illegalStateException) {
                  logger.info(illegalStateException.getMessage());
                } else if (throwable instanceof OrmNotFoundException) {
                  logger.info("Queue is empty !");
                } else {
                  logger.error("Subscriber dropping exception", throwable);
                }
                pgChannel.resume();
              }
            );
        }
      )
      .endHandler(() -> logger.info("pg-channel subscription stopped"))
      .subscribeHandler(() -> logger.info("subscribed to pg-channel"))
      .exceptionHandler(throwable -> logger.error("Error in pg-subscription", throwable));
  }

  private Uni<Void> pollUntilQueueIsEmpty(MessageConsumer messageConsumer, String verticleId) {
    return pollOnce(messageConsumer.queueConfiguration(), verticleId)
      .flatMap(messageConsumer::process)
      .flatMap(avoid -> pollUntilQueueIsEmpty(messageConsumer, verticleId));
  }

  private Uni<List<MessageRecord>> pollOnce(QueueConfiguration configuration, String deploymentId) {
    return queue.query(pollingStatement(configuration, deploymentId)).onFailure(OrmNotFoundException.class)
      .recoverWithUni(
        () -> queue.query(recoveryPollingStatement(configuration, deploymentId))
          .map(messageRecords -> messageRecords.stream().map(m -> m.withState(MessageState.RECOVERY)).toList())
      );
  }

  private String pollingStatement(
    final QueueConfiguration configuration,
    String deploymentId
  ) {
    return "update " + queue.mapper().table() + " set state = 'PROCESSING', verticle_id = '" + deploymentId + "' where id in (" +
      " select id from " + queue.mapper().table() + " where " +
      " state in ('CREATED','SCHEDULED','RETRY')" +
      " and (scheduled is null or scheduled <= current_timestamp)" +
      " and (expiration is null or expiration >= current_timestamp)" +
      " and (retry_counter = 0 or last_update + interval '" + configuration.retryIntervalInMinutes() + " minutes' <= current_timestamp)" +
      " order by priority for update skip locked limit " + configuration.batchSize() +
      " ) returning *;";
  }

  private String recoveryPollingStatement(
    final QueueConfiguration configuration,
    String deploymentId
  ) {
    return "update " + queue.mapper().table() + " set state = 'PROCESSING', verticle_id = '" + deploymentId + "' where id in (" +
      " select id from " + queue.mapper().table() + " where " +
      " state = 'RECOVERY' " +
      " order by priority for update skip locked limit " + configuration.batchSize() +
      " ) returning *;";
  }

}
