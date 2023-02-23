package io.vertx.skeleton.sql.test;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mutiny.core.Vertx;
import io.vertx.skeleton.sql.Repository;
import io.vertx.skeleton.sql.models.QueryOptions;
import io.vertx.skeleton.sql.models.BaseRecord;
import io.vertx.skeleton.test.VertxTestBootstrap;

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
public class RepositoryTests {
  public static final VertxTestBootstrap BOOTSTRAP = new VertxTestBootstrap()
    .setPostgres(true)
    .addLiquibaseRun("sql-test.xml", Map.of())
    .setConfigurationPath("config.json");

  private final Logger LOGGER = LoggerFactory.getLogger(RepositoryTests.class);


  @BeforeAll
  static void prepare() throws Exception {
    BOOTSTRAP.bootstrap();
  }

  @AfterAll
  static void destroy() throws Exception {
    BOOTSTRAP.destroy();
  }


  @Test
  void insert(Vertx vertx, VertxTestContext vertxTestContext) {
    final var repository = new Repository<>(new TestModelMapper(), BOOTSTRAP.REPOSITORY_HANDLER);
    repository.insert(testModel()).await().indefinitely();
    vertxTestContext.completeNow();
  }

  @Test
  void insert_and_select(Vertx vertx, VertxTestContext vertxTestContext) {
    final var repository = new Repository<>(new TestModelMapper(), BOOTSTRAP.REPOSITORY_HANDLER);
    final var model = testModel();
    repository.insert(model).await().indefinitely();
    final var selectedValue = repository.selectByKey(new TestModelKey(model.textField())).await().indefinitely();
    LOGGER.info("Model time ->" + model.timeStampField());
    LOGGER.info("DB time ->" + selectedValue.timeStampField());
    vertxTestContext.completeNow();
  }

  @Test
  void insert_and_query(Vertx vertx, VertxTestContext vertxTestContext) {
    final var repository = new Repository<>(new TestModelMapper(), BOOTSTRAP.REPOSITORY_HANDLER);
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
            .put("name", "reeferman")
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
