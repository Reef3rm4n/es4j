package io.vertx.skeleton.ccp.subscribers;

import io.vertx.skeleton.ccp.models.QueueConfiguration;
import io.vertx.skeleton.orm.RepositoryHandler;
import io.vertx.core.impl.logging.Logger;

import java.util.concurrent.atomic.AtomicLong;


public class PgQueueRefresher {
  public final AtomicLong timer = new AtomicLong();
  private final RepositoryHandler repositoryHandler;
  private final QueueConfiguration configuration;
  private final Logger logger;

  public PgQueueRefresher(
    final RepositoryHandler repositoryHandler,
    final QueueConfiguration configuration,
    final Logger logger
    ) {
    this.repositoryHandler = repositoryHandler;
    this.configuration = configuration;
    this.logger = logger;
  }

  public PgQueueRefresher startRetryTimer(final long throttle) {
    repositoryHandler.vertx().setTimer(
      throttle,
      delay -> {
        logger.info("Running retry refresh timer");
        repositoryHandler.sqlClient().query(retryUpdates(configuration)).execute()
          .subscribe()
          .with(
            avoid -> {
              logger.info("Retry refresh will re-run in " + configuration.retryIntervalInMinutes() + " minutes");
              startRetryTimer(configuration.retryIntervalInMinutes() * 60000);
            },
            throwable -> {
              logger.info("Retry refresh will re-run in " + configuration.retryIntervalInMinutes() + " minutes");
              startRetryTimer(configuration.retryIntervalInMinutes() * 60000);
            }
          );
      }
    );
    return this;
  }

  public PgQueueRefresher startRecoveryTimer(final long throttle) {
    repositoryHandler.vertx().setTimer(
      throttle,
      delay -> {
        logger.info("Running recovery refresh timer");
        repositoryHandler.sqlClient().query(recoveryUpdates(configuration)).execute()
          .subscribe()
          .with(
            avoid -> {
              logger.info("Recovery refresh will re-run in " + configuration.maxProcessingTimeInMinutes() + " minutes");
              startRecoveryTimer(configuration.retryIntervalInMinutes() * 60000);
            },
            throwable -> {
              logger.info("Recovery refresh will re-run in " + configuration.maxProcessingTimeInMinutes() + " minutes");
              startRecoveryTimer(configuration.retryIntervalInMinutes() * 60000);
            }
          );
      }
    );
    return this;
  }

  public PgQueueRefresher startPurgeRefreshTimer(final long throttle) {
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
                startPurgeRefreshTimer(configuration.retryIntervalInMinutes() * 60000);
              },
              throwable -> {
                logger.info("Recovery refresh will re-run in " + configuration.maintenanceEvery() + " minutes");
                startPurgeRefreshTimer(configuration.retryIntervalInMinutes() * 60000);
              }
            );
        }
      );
    }
    return this;
  }


  private static String purgeIdempotency(QueueConfiguration configuration) {
    return "delete from " + configuration.queueName() + "_id where creation_date <= current_timestamp - interval '" + configuration.idempotencyNumberOfDays() + " days'";
  }

  private static String recoveryUpdates(QueueConfiguration configuration) {
    return "update " + configuration.queueName() + " set version = version + 1, state = 'RECOVERY'  where " +
      " state = 'PROCESSING' and last_update + interval '" + configuration.maxProcessingTimeInMinutes() + "minutes' <= current_timestamp;";
  }

  private static String retryUpdates(QueueConfiguration configuration) {
    return "update " + configuration.queueName() + " set version = version + 1  where " +
      " state = 'RETRY' and last_update + interval '" + configuration.retryIntervalInMinutes() + "minutes' <= current_timestamp;";
  }


}
