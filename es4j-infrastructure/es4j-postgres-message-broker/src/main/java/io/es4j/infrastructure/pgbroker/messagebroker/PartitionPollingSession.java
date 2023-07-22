package io.es4j.infrastructure.pgbroker.messagebroker;


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
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class PartitionPollingSession {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionPollingSession.class);
  private final ConcurrentLinkedQueue<MessageRecord> messages = new ConcurrentLinkedQueue<>();
  private final Repository<PartitionKey, MessagePartition, PartitionQuery> partitionRepository;
  private final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue;
  private final AtomicBoolean claimMessages = new AtomicBoolean(false);
  private final AtomicBoolean partitionActive = new AtomicBoolean(true);
  private final PgBrokerConfiguration configuration;
  private final String verticleId;
  private final String partitionId;
  public MessagePartition messagePartition;


  public PartitionPollingSession(
    Repository<PartitionKey, MessagePartition, PartitionQuery> partitionRepository,
    Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue,
    PgBrokerConfiguration configuration,
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
          messagePartition = partition;
          timerTaskDeployer.deploy(partitionHeartBeat());
          timerTaskDeployer.deploy(partitionPolling());
          claimMessages.set(true);
          return Uni.createFrom().voidItem();
        }
      );
  }

  public io.es4j.task.TimerTask partitionPolling() {
    return new io.es4j.task.TimerTask() {
      @Override
      public Uni<Void> performTask() {
        if (claimMessages.get()) {
          claimMessages.set(false);
          LOGGER.info("Something to claim on partition {}", messagePartition);
          if (partitionActive.get()) {
            return pollPartition(partitionId, verticleId, configuration)
              .onFailure().recoverWithNull().replaceWithVoid();
          }
          throw new PartitionTakenException();
        } else {
          LOGGER.debug("Nothing to claim on partition {}", messagePartition);
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

  public io.es4j.task.TimerTask partitionHeartBeat() {
    return new io.es4j.task.TimerTask() {
      @Override
      public Uni<Void> performTask() {
        return partitionRepository.query(
            partitionHeartBeatStatement(),
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
    LOGGER.info("Closing polling session for {}", messagePartition);
    partitionActive.set(false);
    return partitionRepository.updateByKey(messagePartition.release()).replaceWithVoid();
  }

  public List<MessageRecord> pollMessages() {
    final List<MessageRecord> polledMessages = new ArrayList<>();
    MessageRecord message;
    while ((message = messages.poll()) != null) {
      polledMessages.add(message);
    }
    return polledMessages;
  }

  private String pollingStatement() {
    return "update queue set state = 'PROCESSING', verticle_id = #{deploymentId} where message_id in (" +
      " select message_id from queue where " +
      " state in ('CREATED','SCHEDULED','RETRY')" +
      " and partition_id = #{partitionId} " +
      " and (scheduled is null or scheduled <= current_timestamp )" +
      " and (expiration is null or expiration >= current_timestamp )" +
      " and (retry_counter = 0 or updated + interval '#{retryInterval} seconds' <= now() ) " +
      " order by message_sequence for update skip locked limit #{batchSize}" +
      " ) returning *;";
  }

  private String recoveryStatement() {
    return "update queue set state = 'PROCESSING', verticle_id = #{deploymentId} where message_id in (" +
      " select message_id from queue where " +
      " state = 'RECOVERY' and partition_id = #{partitionId} " +
      " order by message_sequence for update skip locked limit #{batchSize}" +
      " ) returning *;";
  }

  private Uni<Void> pollPartition(String partitionId, String deploymentId, PgBrokerConfiguration configuration) {
    final var recoveredMessages = recoverMessages(partitionId, deploymentId, configuration);
    final var claimedMessages = claimMessages(partitionId, deploymentId, configuration);
    return Uni.join().all(recoveredMessages, claimedMessages).andCollectFailures()
      .onFailure().recoverWithNull().replaceWithVoid();
  }

  private Uni<Void> recoverMessages(String partitionId, String deploymentId, PgBrokerConfiguration configuration) {
    // this is a hack to recover messages that are stuck in the PROCESSING state...
    // this can happen if the verticle crashes before the message is committed
    return recoverPartitionMessages(partitionId, deploymentId, configuration)
      .onFailure(NotFound.class).recoverWithItem(Collections::emptyList)
      .map(msg -> {
          LOGGER.info("Recovered {} messages from partitionId {}", msg.size(), partitionId);
          msg.stream().map(m -> m.withState(MessageState.RECOVERY)).forEach(messages::add);
          return msg;
        }
      )
      .replaceWithVoid();
  }

  private Uni<Void> claimMessages(String partitionId, String deploymentId, PgBrokerConfiguration configuration) {
    return messageQueue.query(pollingStatement(), Map.of(
          "partitionId", partitionId,
          "deploymentId", deploymentId,
          "batchSize", configuration.batchSize(),
          "retryInterval", Objects.requireNonNullElse(configuration.retryBackOffInterval(), Duration.ofMinutes(5)).getSeconds()
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

  private Uni<List<MessageRecord>> recoverPartitionMessages(String partitionId, String deploymentId, PgBrokerConfiguration configuration) {
    return messageQueue.query(recoveryStatement(), Map.of(
        "partitionId", partitionId,
        "deploymentId", deploymentId,
        "batchSize", configuration.batchSize()
      )
    );
  }


  private String claimPartitionStatement() {
    return (
      " update queue_partition set updated = now(), locked = true, verticle_id = #{verticleId} where partition_id = #{partitionId} and (locked = false or updated + interval '1 minute' <= now() )  returning *"
    );
  }

  private String partitionHeartBeatStatement() {
    return (
      "update queue_partition set updated = now() where partition_id = #{partitionId} and verticle_id = #{verticleId} and locked = true returning *"
    );
  }

  public Uni<MessagePartition> takePartition(String partitionId, String deploymentId) {
    return partitionRepository.query(claimPartitionStatement(),
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

  public void signalMessage() {
    claimMessages.set(true);
  }
}
