package io.vertx.skeleton.taskqueue.postgres;

import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.skeleton.taskqueue.models.TaskQueueConfiguration;
import io.vertx.core.impl.logging.Logger;
import io.vertx.skeleton.sql.RepositoryHandler;

import java.util.concurrent.atomic.AtomicLong;


public class PgRefresher {
  public final AtomicLong timer = new AtomicLong();
  private final RepositoryHandler repositoryHandler;
  private final TaskQueueConfiguration configuration;
  private static final Logger logger = LoggerFactory.getLogger(PgRefresher.class);

  public PgRefresher(
    final RepositoryHandler repositoryHandler,
    final TaskQueueConfiguration configuration
  ) {
    this.repositoryHandler = repositoryHandler;
    this.configuration = configuration;
  }

  public PgRefresher startRetryTimer(final long throttle) {
    repositoryHandler.vertx().setTimer(
      throttle,
      delay -> {
        logger.info("Running retry refresh timer");
        repositoryHandler.sqlClient().query(retryUpdates(configuration)).execute()
          .subscribe()
          .with(
            avoid -> {
              logger.info("Retry refresh will re-run in " + configuration.retryIntervalInSeconds() + " minutes");
              startRetryTimer(configuration.retryIntervalInSeconds() * 10000);
            },
            throwable -> {
              logger.info("Retry refresh will re-run in " + configuration.retryIntervalInSeconds() + " minutes");
              startRetryTimer(configuration.retryIntervalInSeconds() * 10000);
            }
          );
      }
    );
    return this;
  }

  public PgRefresher startRecoveryTimer(final long throttle) {
    repositoryHandler.vertx().setTimer(
      throttle,
      delay -> {
        logger.info("Running recovery refresh timer");
        repositoryHandler.sqlClient().query(recoveryUpdates(configuration)).execute()
          .subscribe()
          .with(
            avoid -> {
              logger.info("Recovery refresh will re-run in " + configuration.maxProcessingTimeInMinutes() + " minutes");
              startRecoveryTimer(configuration.retryIntervalInSeconds() * 60000);
            },
            throwable -> {
              logger.info("Recovery refresh will re-run in " + configuration.maxProcessingTimeInMinutes() + " minutes");
              startRecoveryTimer(configuration.retryIntervalInSeconds() * 60000);
            }
          );
      }
    );
    return this;
  }

  public PgRefresher startPurgeRefreshTimer(final long throttle) {
    if (configuration.idempotentProcessors()) {
      repositoryHandler.vertx().setTimer(
        throttle,
        delay -> {
          logger.info("Running purge refresh timer");
          repositoryHandler.sqlClient().query(purgeIdempotency(configuration)).execute()
            .subscribe()
            .with(
              avoid -> {
                logger.info("Recovery refresh will re-run in " + configuration.maintenanceEvery() + " minutes");
                startPurgeRefreshTimer(configuration.retryIntervalInSeconds() * 1000);
              },
              throwable -> {
                logger.info("Recovery refresh will re-run in " + configuration.maintenanceEvery() + " minutes");
                startPurgeRefreshTimer(configuration.retryIntervalInSeconds() * 1000);
              }
            );
        }
      );
    }
    return this;
  }


  private static String purgeIdempotency(TaskQueueConfiguration configuration) {
    return "delete from  task_queue_tx where inserted <= current_timestamp - interval '" + configuration.idempotencyNumberOfDays() + " days'";
  }
  // todo instead of purging should move to another database.

  private static String recoveryUpdates(TaskQueueConfiguration configuration) {
    return "update task_queue set rec_version = version + 1, state = 'RECOVERY'  where " +
      " state = 'PROCESSING' and updated + interval '" + configuration.maxProcessingTimeInMinutes() + "minutes' <= current_timestamp;";
  }

  private static String retryUpdates(TaskQueueConfiguration configuration) {
    return "update task_queue set rec_version = rec_version + 1  where " +
      " state = 'RETRY' and updated + interval '" + configuration.retryIntervalInSeconds() + "seconds' <= current_timestamp;";
  }


}
