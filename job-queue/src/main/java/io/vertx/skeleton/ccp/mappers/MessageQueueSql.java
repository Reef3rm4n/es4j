package io.vertx.skeleton.ccp.mappers;

import io.vertx.skeleton.ccp.QueueDeployer;
import io.vertx.skeleton.ccp.models.QueueConfiguration;
import io.vertx.skeleton.ccp.subscribers.PgQueueRefresher;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.skeleton.sql.LiquibaseHandler;
import io.vertx.skeleton.sql.RepositoryHandler;
import io.vertx.skeleton.sql.misc.Constants;
import io.vertx.skeleton.sql.misc.EnvVars;

import java.util.Map;

public class MessageQueueSql {

  private MessageQueueSql(){}


  public static Uni<Void> bootstrapQueue(final RepositoryHandler repositoryHandler) {
    if (QueueDeployer.QUEUES_CHECKED.compareAndSet(false, true)) {
      final var queueConfiguration = repositoryHandler.configuration().getJsonObject("job-queue").mapTo(QueueConfiguration.class);
      return liquibase(repositoryHandler)
        .invoke(avoid -> bootstrapQueueRefresher(repositoryHandler, queueConfiguration));
    }
    return Uni.createFrom().voidItem();
  }

  private static Uni<Void> liquibase(RepositoryHandler repositoryHandler) {
    return LiquibaseHandler.liquibaseString(
      repositoryHandler,
      "task-queue.xml",
      Map.of(
        "schema", repositoryHandler.configuration().getString(Constants.SCHEMA, EnvVars.SCHEMA)
      )
    );
  }

  private static PgQueueRefresher bootstrapQueueRefresher(RepositoryHandler repositoryHandler, QueueConfiguration queueConfiguration) {
    return new PgQueueRefresher(
      repositoryHandler,
      queueConfiguration,
      LoggerFactory.getLogger(queueConfiguration.queueName())
    )
      .startRetryTimer(10000L)
      .startRecoveryTimer(10000L)
      .startPurgeRefreshTimer(10000L);
  }

  public static String camelToSnake(String str) {
    // Regular Expression
    String regex = "([a-z])([A-Z]+)";

    // Replacement string
    String replacement = "$1_$2";

    // Replace the given regex
    // with replacement string
    // and convert it to lower case.
    str = str
      .replaceAll(
        regex, replacement)
      .toLowerCase();

    // return string
    return str;
  }

}
