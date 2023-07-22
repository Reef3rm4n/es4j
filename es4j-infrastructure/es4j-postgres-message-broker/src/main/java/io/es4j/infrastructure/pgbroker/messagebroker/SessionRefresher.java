package io.es4j.infrastructure.pgbroker.messagebroker;

import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.sql.Repository;
import io.es4j.task.LockLevel;
import io.es4j.task.TimerTask;
import io.es4j.task.TimerTaskConfiguration;
import io.es4j.task.TimerTaskDeployer;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

public class SessionRefresher {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionRefresher.class);

    public static void refreshTimers(ConsumerManager consumerManager, TimerTaskDeployer timerTasks, Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue) {
        timerTasks.deploy(
            new TimerTask() {
                @Override
                public Uni<Void> performTask() {
                    LOGGER.info("Retry interval refresher");
                    return messageQueue.repositoryHandler().sqlClient().query(initiateRetry(consumerManager.pgBrokerConfiguration())).execute()
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
            }

        );
        timerTasks.deploy(
            new TimerTask() {
                @Override
                public Uni<Void> performTask() {
                    LOGGER.info("Trying to recover lost messages");
                    return messageQueue.repositoryHandler().sqlClient().query(recoverLostMessages(consumerManager.pgBrokerConfiguration())).execute()
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
            }
        );
        if (consumerManager.pgBrokerConfiguration().idempotency()) {
            timerTasks.deploy(
                new TimerTask() {
                    @Override
                    public Uni<Void> performTask() {
                        LOGGER.info("Purging tx table");
                        return messageQueue.repositoryHandler().sqlClient().query(purgeTransactions(consumerManager.pgBrokerConfiguration())).execute()
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
                }
            );
        }

    }

    public static String purgeTransactions(PgBrokerConfiguration configuration) {
        return "delete from  queue_tx where inserted <= current_timestamp - interval '" + configuration.purgeTxAfter().toDays() + " days'";
    }

    public static String recoverLostMessages(PgBrokerConfiguration configuration) {
        return "update queue set rec_version = rec_version + 1, state = 'RECOVERY'  where " +
            " state = 'PROCESSING' and updated + interval '" + configuration.maxProcessingTime().getSeconds() + " seconds' <= current_timestamp;";
    }

    public static String initiateRetry(PgBrokerConfiguration configuration) {
        return "update queue set rec_version = rec_version + 1  where " +
            " state = 'RETRY' and updated + interval '" + configuration.retryBackOffInterval().getSeconds() + " seconds' <= current_timestamp;";
    }
}
