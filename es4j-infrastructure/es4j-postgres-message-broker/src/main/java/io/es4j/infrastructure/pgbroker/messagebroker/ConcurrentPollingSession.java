package io.es4j.infrastructure.pgbroker.messagebroker;


import io.es4j.infrastructure.pgbroker.exceptions.QueueEmptyException;
import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.sql.Repository;
import io.es4j.sql.exceptions.NotFound;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;


public class ConcurrentPollingSession {
  private final ConcurrentLinkedQueue<MessageRecord> messages = new ConcurrentLinkedQueue<>();
  private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentPollingSession.class);
  private final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue;
  private final PgBrokerConfiguration configuration;
  private final String deploymentId;
  private final AtomicBoolean claimMessages = new AtomicBoolean(false);

  public ConcurrentPollingSession(
    Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue,
    PgBrokerConfiguration configuration,
    String deploymentId
  ) {
    this.messageQueue = messageQueue;
    this.configuration = configuration;
    this.deploymentId = deploymentId;
  }

  public List<MessageRecord> pollMessages() {
    final List<MessageRecord> polledMessages = new ArrayList<>();
    MessageRecord message;
    while ((message = messages.poll()) != null) {
      polledMessages.add(message);
    }
    return polledMessages;
  }


  public io.es4j.task.TimerTask provideTask() {
    return () -> {
      if (claimMessages.get()) {
        claimMessages.set(false);
        LOGGER.debug("Checking queue");
        final var params = Map.<String, Object>of(
          "deploymentId", Objects.requireNonNullElse(deploymentId, UUID.randomUUID().toString()),
          "batchSize", Objects.requireNonNullElse(configuration.batchSize(), 10),
          "retryInterval", Objects.requireNonNullElse(configuration.retryBackOffInterval(), Duration.ofMinutes(5)).getSeconds()
        );
        final var messagesClaimed = claimMessages(params);
        final var messagesRecovered = recoverStuckMessages(params);
        return Uni.join().all(messagesClaimed, messagesRecovered).andCollectFailures()
          .onFailure().recoverWithNull().replaceWithVoid()
          .eventually(() -> {
            if (messages.isEmpty()) {
              LOGGER.debug("Messages not found");
            } else {
              LOGGER.info("Found {} messages", messages.size());
            }
          });
      } else {
        LOGGER.debug("Nothing to check");
        return Uni.createFrom().voidItem();
      }
    };
  }

  private Uni<Void> claimMessages(Map<String, Object> params) {
    return messageQueue.query(pollingStatement(), params)
      .map(messageRecords -> {
        LOGGER.debug("Claimed {} messages", messageRecords.size());
        messages.addAll(messageRecords);
        return messageRecords;
      })
      .onFailure(NotFound.class)
      .recoverWithNull()
      .replaceWithVoid();
  }

  private Uni<Void> recoverStuckMessages(Map<String, Object> params) {
    // this is a hack to recover messages that are stuck in the PROCESSING state...
    // this can happen if the verticle crashes before the message is committed
    return messageQueue.query(recoveryStatement(), params)
      .map(messageRecords -> {
        messages.addAll(messageRecords.stream().map(m -> m.withState(MessageState.RECOVERY)).toList());
        LOGGER.debug("Recovered {} messages", messageRecords.size());
        return messageRecords;
      })
      .onFailure(NotFound.class).recoverWithNull()
      .replaceWithVoid();
  }

  protected String pollingStatement() {
//    updated + interval '" + configuration.retryBackOffInterval().getSeconds() + " seconds'
    return "update queue set state = 'PROCESSING', verticle_id = #{deploymentId} where message_id in (" +
      " select message_id from queue where " +
      " state in ('CREATED','SCHEDULED','RETRY')" +
      " and partition_id = 'none' " +
      " and (scheduled is null or scheduled <= current_timestamp)" +
      " and (expiration is null or expiration >= current_timestamp)" +
      " and (retry_counter = 0 or updated + interval '#{retryInterval} seconds' <= now() ) " +
      " order by priority for update skip locked limit #{batchSize} " +
      " ) returning *;";
  }

  protected String recoveryStatement() {
    return "update queue set state = 'PROCESSING', verticle_id = #{deploymentId} where message_id in (" +
      " select message_id from queue where " +
      " state = 'RECOVERY' " +
      " and partition_id = 'none' " +
      " order by priority for update skip locked limit #{batchSize}" +
      " ) returning *;";
  }


  public void signalMessage() {
    claimMessages.set(true);
  }
}
