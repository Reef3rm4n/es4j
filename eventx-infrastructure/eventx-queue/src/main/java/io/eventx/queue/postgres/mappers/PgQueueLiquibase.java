package io.eventx.queue.postgres.mappers;

import io.eventx.sql.LiquibaseHandler;
import io.eventx.sql.RepositoryHandler;
import io.eventx.sql.misc.Constants;
import io.eventx.sql.misc.EnvVars;
import io.eventx.queue.models.QueueConfiguration;
import io.eventx.queue.postgres.PgRefresher;
import io.smallrye.mutiny.Uni;
import io.eventx.queue.postgres.PgTaskSubscriber;

import java.util.Map;

public class PgQueueLiquibase {

  private PgQueueLiquibase(){}


  public static Uni<Void> bootstrapQueue(final RepositoryHandler repositoryHandler, QueueConfiguration configuration) {
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

  private static PgRefresher bootstrapQueueRefresher(RepositoryHandler repositoryHandler, QueueConfiguration queueConfiguration) {
    return new PgRefresher(
      repositoryHandler,
            queueConfiguration
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
