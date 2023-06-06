package io.eventx.infrastructure;

import io.eventx.InfrastructureBootstrap;
import io.eventx.sql.LiquibaseHandler;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mutiny.core.Vertx;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

@ExtendWith(VertxExtension.class)
public class LiquibaseTest {
  private final static InfrastructureBootstrap bootstrap = new InfrastructureBootstrap()
    .setPostgres(true);

  @Test
  void test_queue_changelog(Vertx vertx, VertxTestContext vertxTestContext) {
    bootstrap.configuration();
    bootstrap.configuration().put("schema", "testschema");
    bootstrap.deployPgContainer();
    InfrastructureBootstrap.REPOSITORY_HANDLER.sqlClient().query("create schema if not exists testschema").execute()
      .await().indefinitely();
    LiquibaseHandler.liquibaseString(
      InfrastructureBootstrap.REPOSITORY_HANDLER,
      "queue.xml",
      Map.of("schema", "testschema", "queueName", "test_queue")
    ).await().indefinitely();
    vertxTestContext.completeNow();
  }

//  @Test
//  void test_queue_changelog_local_db(Vertx vertx, VertxTestContext vertxTestContext) {
//    bootstrap.configuration();
//    final var configuration = new JsonObject().put("schema", "testschema");
//    final var mainRepositoryHandler = RepositoryHandler.leasePool(configuration, vertx);
//    mainRepositoryHandler.sqlClient().query("create schema if not exists testschema").execute()
//      .await().indefinitely();
//    mainRepositoryHandler.vertx().fileSystem().readFile("queue.xml")
//      .flatMap(buffer -> LiquibaseHandler.liquibaseString(
//          vertx,
//          configuration,
//          buffer,
//          Map.of("schema", "testschema", "queueName", "test_queue")
//        )
//      )
//      .await().indefinitely();
//    vertxTestContext.completeNow();
//  }
//@Test
//  void test_cfg_changelog_local_db(Vertx vertx, VertxTestContext vertxTestContext) {
//    bootstrap.configuration();
//    final var configuration = new JsonObject().put("schema", "testschema");
//    final var mainRepositoryHandler = RepositoryHandler.leasePool(configuration, vertx);
//    mainRepositoryHandler.sqlClient().query("create schema if not exists testschema").execute()
//      .await().indefinitely();
//    mainRepositoryHandler.vertx().fileSystem().readFile("config.xml")
//      .flatMap(buffer -> LiquibaseHandler.liquibaseString(
//          vertx,
//          configuration,
//          buffer,
//          Map.of("schema", "testschema")
//        )
//      )
//      .await().indefinitely();
//    vertxTestContext.completeNow();
//  }

  @Test
  void test_config_changelog(Vertx vertx, VertxTestContext vertxTestContext) {
    bootstrap.configuration();
    bootstrap.CONFIGURATION.put("schema", "testschema");
    bootstrap.deployPgContainer();
    InfrastructureBootstrap.REPOSITORY_HANDLER.sqlClient().query("create schema if not exists testschema").execute()
      .await().indefinitely();
    LiquibaseHandler.liquibaseString(
      InfrastructureBootstrap.REPOSITORY_HANDLER,
      "config.xml",
      Map.of("schema", "testschema")
    ).await().indefinitely();
    vertxTestContext.completeNow();
  }
}
