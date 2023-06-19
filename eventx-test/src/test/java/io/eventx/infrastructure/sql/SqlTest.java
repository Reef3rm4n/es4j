package io.eventx.infrastructure.sql;

import io.eventx.InfrastructureBootstrap;
import io.eventx.sql.LiquibaseHandler;
import io.eventx.sql.Repository;
import io.eventx.sql.models.BaseRecord;
import io.eventx.sql.models.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mutiny.core.Vertx;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(VertxExtension.class)
public class SqlTest {
  public static final InfrastructureBootstrap BOOTSTRAP = new InfrastructureBootstrap()
    .addLiquibaseRun("sql-test.xml",Map.of("schema", "eventx"))
    .setPostgres(true);

  private final Logger LOGGER = LoggerFactory.getLogger(SqlTest.class);

  @AfterAll
  static void destroy() throws Exception {
    BOOTSTRAP.destroy();
  }

  @BeforeAll
  static void start() {
    BOOTSTRAP.start();
  }


  @Test
  void test_sql_migration() {
    BOOTSTRAP.configuration();
    BOOTSTRAP.configuration().put("schema", "eventx");
    BOOTSTRAP.deployPgContainer();
    LiquibaseHandler.liquibaseString(
      InfrastructureBootstrap.REPOSITORY_HANDLER,
      "sql-test.xml",
      Map.of("schema", "eventx")
    ).await().indefinitely();
  }

  @Test
  void insert(Vertx vertx, VertxTestContext vertxTestContext) {
    final var repository = new Repository<>(new TestModelMapper(), InfrastructureBootstrap.REPOSITORY_HANDLER);
    repository.insert(testModel()).await().indefinitely();
    vertxTestContext.completeNow();
  }

  @Test
  void insert_and_select(Vertx vertx, VertxTestContext vertxTestContext) {
    final var repository = new Repository<>(new TestModelMapper(), InfrastructureBootstrap.REPOSITORY_HANDLER);
    final var model = testModel();
    repository.insert(model).await().indefinitely();
    final var selectedValue = repository.selectByKey(new TestModelKey(model.textField())).await().indefinitely();
    LOGGER.info("Model time ->" + model.timeStampField());
    LOGGER.info("DB time ->" + selectedValue.timeStampField());
    vertxTestContext.completeNow();
  }

  @Test
  void insert_and_query(Vertx vertx, VertxTestContext vertxTestContext) {
    final var repository = new Repository<>(new TestModelMapper(), InfrastructureBootstrap.REPOSITORY_HANDLER);
    final var model = IntStream.range(0, 20).mapToObj(
      integerValue -> testModel()
    ).toList();
    repository.insertBatch(model).await().indefinitely();
    final var models = repository.query(
      new TestModelQuery(
        model.stream().map(TestModel::textField).toList(),
        Instant.now().minus(1, ChronoUnit.MINUTES),
        Instant.now().plus(1, ChronoUnit.MINUTES),
        0L,
        10L,
        model.stream().map(TestModel::longField).findFirst().orElse(0L),
        List.of("reefer*"),
        QueryOptions.simple("default")
      )
    ).await().indefinitely();
    assertEquals(model.size(), models.size());
    vertxTestContext.completeNow();
  }



  private static TestModel testModel() {
    return new TestModel(
      UUID.randomUUID().toString(),
      Instant.now(),
      new JsonObject()
        .put("person", new JsonObject()
          .put("data", "value")
          .put("details", new JsonObject()
            .put("fileName", "reeferman")
            .put("lastname", "benato")
          )
        )
      ,
      0L,
      1,
      BaseRecord.newRecord()
    );
  }
}
