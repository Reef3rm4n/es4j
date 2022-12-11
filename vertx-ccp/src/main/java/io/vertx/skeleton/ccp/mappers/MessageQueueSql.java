package io.vertx.skeleton.ccp.mappers;

import io.vertx.skeleton.ccp.QueueConsumerVerticle;
import io.vertx.skeleton.ccp.QueueDeployer;
import io.vertx.skeleton.ccp.models.QueueConfiguration;
import io.vertx.skeleton.ccp.subscribers.PgQueueRefresher;
import io.vertx.skeleton.orm.LiquibaseHandler;
import io.vertx.skeleton.orm.RepositoryHandler;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.templates.RowMapper;

import java.util.List;
import java.util.Map;

public class MessageQueueSql {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueueConsumerVerticle.class);

  public static Uni<Void> bootstrapQueues(
    final RepositoryHandler repositoryHandler,
    final List<QueueConfiguration> queueConfigurations
  ) {
    if (QueueDeployer.QUEUES_CHECKED.compareAndSet(false, true)) {
      return Multi.createFrom().iterable(queueConfigurations)
        .onItem().transformToUniAndMerge(queueConfiguration -> {
            LOGGER.info("Queue configuration -> " + JsonObject.mapFrom(queueConfiguration).encodePrettily());
            final var config = new JsonObject()
              .put("tableName", queueConfiguration.queueName())
              .put("schema", repositoryHandler.configuration().getValue("schema"));
            final RowMapper<Boolean> mapper = RowMapper.newInstance(
              row -> row.getBoolean("EXISTS")
            );
            return liquibase(repositoryHandler, queueConfiguration)
              .invoke(avoid -> bootstrapQueueRefresher(repositoryHandler, queueConfiguration));
          }
        ).collect().asList()
        .replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }

  private static Uni<Void> liquibase(RepositoryHandler repositoryHandler, QueueConfiguration queueConfiguration) {
    return LiquibaseHandler.liquibaseString(
      repositoryHandler,
      "queue.xml",
      Map.of(
        "queueName", queueConfiguration.queueName(),
        "schema", repositoryHandler.configuration().getString("schema")
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
