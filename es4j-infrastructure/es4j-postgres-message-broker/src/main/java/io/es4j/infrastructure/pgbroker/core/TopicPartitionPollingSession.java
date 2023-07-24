package io.es4j.infrastructure.pgbroker.core;


import io.es4j.infrastructure.pgbroker.exceptions.PartitionTakenException;
import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.sql.Repository;
import io.es4j.sql.exceptions.NotFound;
import io.es4j.task.LockLevel;
import io.es4j.task.TimerTaskConfiguration;
import io.es4j.task.TimerTaskDeployer;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class TopicPartitionPollingSession {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionPollingSession.class);
  public static final String POLLING_STATEMENT = "update message_broker set state = 'CONSUMING', verticle_id = #{deploymentId} where message_id in (" +
    " select message_id from message_broker where " +
    " state in ('PUBLISHED', 'STUCK') " +
    " and partition_id = #{partitionId} " +
    " order by message_sequence for update skip locked limit #{brokerBatchingSize}" +
    " ) returning *;";
  public static final String CLAIM_PARTITION_STATEMENT = " update message_broker_partition set updated = now(), locked = true, verticle_id = #{verticleId} where partition_id = #{partitionId} and (locked = false or updated + interval '1 minute' <= now() )  returning *";
  public static final String HEART_BEAT_STATEMENT = "update message_broker_partition set updated = now() where partition_id = #{partitionId} and verticle_id = #{verticleId} and locked = true returning *";
  private final ConcurrentLinkedQueue<MessageRecord> messages = new ConcurrentLinkedQueue<>();
  private final Repository<PartitionKey, BrokerPartitionRecord, PartitionQuery> partitionRepository;
  private final Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageQueue;
  private final AtomicBoolean claimMessages = new AtomicBoolean(false);
  private final AtomicBoolean partitionActive = new AtomicBoolean(true);
  private final BrokerConfiguration configuration;
  private final String verticleId;
  private final String partitionId;
  public BrokerPartitionRecord brokerPartitionRecord;


  public TopicPartitionPollingSession(
    Repository<PartitionKey, BrokerPartitionRecord, PartitionQuery> partitionRepository,
    Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageQueue,
    BrokerConfiguration configuration,
    String verticleId,
    String partitionId
  ) {
    this.partitionRepository = partitionRepository;
    this.messageQueue = messageQueue;
    this.configuration = configuration;
    this.verticleId = verticleId;
    this.partitionId = partitionId;
  }

  public Uni<Void> start(TimerTaskDeployer timerTaskDeployer) {
    return takePartition(partitionId, verticleId)
      .onFailure(PartitionTakenException.class).retry().withBackOff(Duration.ofSeconds(30)).atMost(1)
      .flatMap(
        partition -> {
          brokerPartitionRecord = partition;
          timerTaskDeployer.deploy(heartBeatTimer());
          timerTaskDeployer.deploy(pollingTimer());
          claimMessages.set(true);
          return Uni.createFrom().voidItem();
        }
      );
  }

  public io.es4j.task.TimerTask pollingTimer() {
    return new io.es4j.task.TimerTask() {
      @Override
      public Uni<Void> performTask() {
        if (claimMessages.get()) {
          claimMessages.set(false);
          LOGGER.info("Something to claim on partition {}", brokerPartitionRecord);
          if (partitionActive.get()) {
            return claimMessages(partitionId, verticleId, configuration)
              .onFailure().recoverWithNull().replaceWithVoid();
          }
          throw new PartitionTakenException();
        }
        return Uni.createFrom().voidItem();
      }

      @Override
      public TimerTaskConfiguration configuration() {
        return new TimerTaskConfiguration(
          LockLevel.NONE,
          Duration.ofMillis(25),
          Duration.ofMillis(25),
          Duration.ofMillis(25),
          Optional.of(PartitionTakenException.class)
        );
      }

    };
  }

  public io.es4j.task.TimerTask heartBeatTimer() {
    return new io.es4j.task.TimerTask() {
      @Override
      public Uni<Void> performTask() {
        return partitionRepository.query(
            HEART_BEAT_STATEMENT,
            Map.of(
              "partitionId", partitionId,
              "verticleId", verticleId
            )
          )
          .onFailure().transform(
            Unchecked.function((throwable) -> {
                LOGGER.error("Lost partition, heartbeat interrupted");
                partitionActive.set(false);
                throw new PartitionTakenException();
              }
            )
          ).replaceWithVoid();
      }

      @Override
      public TimerTaskConfiguration configuration() {
        return new TimerTaskConfiguration(
          LockLevel.NONE,
          Duration.ofSeconds(30),
          Duration.ofSeconds(30),
          Duration.ofSeconds(30),
          Optional.of(PartitionTakenException.class)
        );
      }
    };
  }

  public Uni<Void> close() {
    LOGGER.info("Closing polling session for {}", brokerPartitionRecord);
    partitionActive.set(false);
    return partitionRepository.updateByKey(brokerPartitionRecord.release()).replaceWithVoid();
  }

  public void poll() {
    claimMessages.set(true);
  }

  public List<MessageRecord> flush() {
    final List<MessageRecord> polledMessages = new ArrayList<>();
    MessageRecord message;
    while ((message = messages.poll()) != null) {
      polledMessages.add(message);
    }
    return polledMessages;
  }

  private Uni<Void> claimMessages(String partitionId, String deploymentId, BrokerConfiguration configuration) {
    return messageQueue.query(POLLING_STATEMENT, Map.of(
          "partitionId", partitionId,
          "deploymentId", deploymentId,
          "brokerBatchingSize", configuration.brokerBatchingSize()
        )
      )
      .onFailure(NotFound.class).recoverWithItem(Collections::emptyList)
      .map(msg -> {
          LOGGER.info("Claimed {} messages from partitionId {}", msg.size(), partitionId);
          messages.addAll(msg);
          return msg;
        }
      )
      .replaceWithVoid();
  }

  public Uni<BrokerPartitionRecord> takePartition(String partitionId, String deploymentId) {
    return partitionRepository.query(CLAIM_PARTITION_STATEMENT,
        Map.of(
          "verticleId", deploymentId,
          "partitionId", partitionId
        )
      )
      .onFailure().transform(Unchecked.function(throwable -> {
        if (throwable instanceof NotFound) {
          LOGGER.info("Partition not available {}", partitionId);
        } else {
          LOGGER.info("Partition claiming dropped {} ", partitionId, throwable);
        }
        throw new PartitionTakenException();
      }))
      .map(partition -> partition.stream().findFirst().orElseThrow());
  }

}
