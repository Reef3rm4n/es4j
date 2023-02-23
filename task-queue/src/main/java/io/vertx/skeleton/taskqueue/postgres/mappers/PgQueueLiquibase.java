package io.vertx.skeleton.taskqueue.postgres.mappers;

import io.vertx.skeleton.taskqueue.models.TaskQueueConfiguration;
import io.vertx.skeleton.taskqueue.postgres.PgRefresher;
import io.smallrye.mutiny.Uni;
import io.vertx.skeleton.sql.LiquibaseHandler;
import io.vertx.skeleton.sql.RepositoryHandler;
import io.vertx.skeleton.sql.misc.Constants;
import io.vertx.skeleton.sql.misc.EnvVars;
import io.vertx.skeleton.taskqueue.postgres.PgTaskSubscriber;

import java.util.Map;

public class PgQueueLiquibase {

  private PgQueueLiquibase(){}


  public static Uni<Void> bootstrapQueue(final RepositoryHandler repositoryHandler, TaskQueueConfiguration configuration) {
    if (PgTaskSubscriber.LIQUIBASE_DEPLOYED.compareAndSet(false, true)) {
      return liquibase(repositoryHandler)
        .invoke(avoid -> bootstrapQueueRefresher(repositoryHandler, configuration));
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

  private static PgRefresher bootstrapQueueRefresher(RepositoryHandler repositoryHandler, TaskQueueConfiguration taskQueueConfiguration) {
    return new PgRefresher(
      repositoryHandler,
      taskQueueConfiguration
    )
      .startRetryTimer(1L)
      .startRecoveryTimer(1L)
      .startPurgeRefreshTimer(1L);
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
