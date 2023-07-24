package io.es4j.infrastructure.pgbroker.core;

import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.sql.Repository;
import io.es4j.sql.exceptions.NotFound;
import io.es4j.task.LockLevel;
import io.es4j.task.TimerTask;
import io.es4j.task.TimerTaskConfiguration;
import io.es4j.task.TimerTaskDeployer;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class SessionRefresher {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionRefresher.class);
  public static final String TX_PURGE_STATEMENT = "delete from message_broker_tx where inserted <= current_timestamp - interval '%s days'";
  public static final String MESSAGE_PURGE_STATEMENT = "delete from message_broker where state = 'CONSUMED' and inserted <= current_timestamp - interval '%s days'";
  public static final String STUCK_MESSAGES_STATEMENT = "update message_broker set rec_version = rec_version + 1, state = 'STUCK'  where state = 'CONSUMING' and updated + interval '%s seconds' <= current_timestamp;";

  public static void refreshTimers(
    final ConsumerRouter consumerRouter,
    final TimerTaskDeployer timerTasks,
    final Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageBroker,
    final Repository<ConsumerTransactionKey, ConsumerTransactionRecord, ConsumerTransactionQuery> messageTx
  ) {
    timerTasks.deploy(queueDurability(consumerRouter, messageBroker));
    timerTasks.deploy(recoverStuckMessagesTask(consumerRouter, messageBroker));
    timerTasks.deploy(messagesTxPurge(consumerRouter, messageTx));
  }

  private static TimerTask queueDurability(
    final ConsumerRouter consumerRouter,
    final Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageBroker
  ) {
    return new TimerTask() {
      @Override
      public Uni<Void> performTask() {
        return messageBroker.query(
            MESSAGE_PURGE_STATEMENT.formatted(consumerRouter.brokerConfiguration().messageDurability().toDays())
          )
          .onFailure(NotFound.class).recoverWithNull()
          .onFailure().invoke(throwable -> LOGGER.error("Error purging messages", throwable))
          .replaceWithVoid();
      }

      @Override
      public TimerTaskConfiguration configuration() {
        return new TimerTaskConfiguration(
          LockLevel.CLUSTER_WIDE,
          Duration.ofMinutes(15),
          Duration.ofMinutes(15),
          Duration.ofMinutes(15),
          Optional.empty()
        );
      }
    };
  }

  private static TimerTask messagesTxPurge(
    final ConsumerRouter consumerRouter,
    final Repository<ConsumerTransactionKey, ConsumerTransactionRecord, ConsumerTransactionQuery> messageTx
  ) {
    return new TimerTask() {
      @Override
      public Uni<Void> performTask() {
        return messageTx.query(TX_PURGE_STATEMENT.formatted(consumerRouter.brokerConfiguration().consumerTxDurability().toDays()))
          .onFailure(NotFound.class).recoverWithNull()
          .onFailure().invoke(throwable -> LOGGER.error("Error purging broker transactions", throwable))
          .replaceWithVoid();
      }

      @Override
      public TimerTaskConfiguration configuration() {
        return new TimerTaskConfiguration(
          LockLevel.CLUSTER_WIDE,
          Duration.ofHours(4),
          Duration.ofHours(4),
          Duration.ofHours(4),
          Optional.empty()
        );
      }
    };
  }

  private static TimerTask recoverStuckMessagesTask(
    final ConsumerRouter consumerRouter,
    final Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageBroker
  ) {
    return new TimerTask() {
      @Override
      public Uni<Void> performTask() {
        return messageBroker.query(
            STUCK_MESSAGES_STATEMENT.formatted(consumerRouter.brokerConfiguration().messageMaxProcessingTime().getSeconds())
          )
          .onFailure(NotFound.class).recoverWithNull()
          .onFailure().invoke(throwable -> LOGGER.error("Error releasing stuck messages", throwable))
          .replaceWithVoid();
      }

      @Override
      public TimerTaskConfiguration configuration() {
        return new TimerTaskConfiguration(
          LockLevel.CLUSTER_WIDE,
          Duration.ofMinutes(15),
          Duration.ofMinutes(15),
          Duration.ofMinutes(15),
          Optional.empty()
        );
      }
    };
  }

}
