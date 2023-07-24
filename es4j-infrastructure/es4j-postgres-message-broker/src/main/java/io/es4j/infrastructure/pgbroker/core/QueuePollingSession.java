package io.es4j.infrastructure.pgbroker.core;


import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.sql.Repository;
import io.es4j.sql.exceptions.NotFound;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;


public class QueuePollingSession {
  public static final String POLLING_STATEMENT = "update message_broker set state = 'CONSUMING', verticle_id = #{deploymentId} where message_id in (" +
    " select message_id from message_broker where " +
    " state in ('PUBLISHED', 'STUCK') " +
    " and partition_id = 'none' " +
    " and (scheduled is null or scheduled <= current_timestamp)" +
    " order by priority for update skip locked limit #{brokerBatchingSize} " +
    " ) returning *;";
  private final ConcurrentLinkedQueue<MessageRecord> messages = new ConcurrentLinkedQueue<>();
  private static final Logger LOGGER = LoggerFactory.getLogger(QueuePollingSession.class);
  private final Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageQueue;
  private final BrokerConfiguration configuration;
  private final String deploymentId;
  private final AtomicBoolean claimMessages = new AtomicBoolean(false);

  public QueuePollingSession(
    Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageQueue,
    BrokerConfiguration configuration,
    String deploymentId
  ) {
    this.messageQueue = messageQueue;
    this.configuration = configuration;
    this.deploymentId = deploymentId;
  }

  public List<MessageRecord> flush() {
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
        LOGGER.debug("Checking address");
        final var params = Map.<String, Object>of(
          "deploymentId", Objects.requireNonNullElse(deploymentId, UUID.randomUUID().toString()),
          "brokerBatchingSize", Objects.requireNonNullElse(configuration.brokerBatchingSize(), 10)
        );
        return claimMessages(params)
          .onFailure().recoverWithNull().replaceWithVoid()
          .eventually(() -> {
            if (messages.isEmpty()) {
              LOGGER.debug("Messages not found");
            } else {
              LOGGER.info("Found {} messages", messages.size());
            }
          });
      } else {
        return Uni.createFrom().voidItem();
      }
    };
  }

  private Uni<Void> claimMessages(Map<String, Object> params) {
    return messageQueue.query(POLLING_STATEMENT, params)
      .map(messageRecords -> {
        LOGGER.debug("Claimed {} messages", messageRecords.size());
        messages.addAll(messageRecords);
        return messageRecords;
      })
      .onFailure(NotFound.class)
      .recoverWithNull()
      .replaceWithVoid();
  }


  public void signalMessage() {
    claimMessages.set(true);
  }
}
