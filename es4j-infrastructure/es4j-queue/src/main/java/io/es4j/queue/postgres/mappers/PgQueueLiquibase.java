package io.es4j.queue.postgres.mappers;

import io.es4j.sql.LiquibaseHandler;
import io.es4j.sql.RepositoryHandler;
import io.es4j.sql.misc.Constants;
import io.es4j.sql.misc.EnvVars;
import io.es4j.queue.models.QueueConfiguration;
import io.es4j.queue.postgres.PgRefresher;
import io.smallrye.mutiny.Uni;
import io.es4j.queue.postgres.PgTaskSubscriber;

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
