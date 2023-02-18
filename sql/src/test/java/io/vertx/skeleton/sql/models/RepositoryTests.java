package io.vertx.skeleton.sql.models;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mutiny.core.Vertx;
import io.vertx.skeleton.sql.Repository;
import io.vertx.skeleton.test.VertxTestBootstrap;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
public class RepositoryTests {
  public static final VertxTestBootstrap BOOTSTRAP = new VertxTestBootstrap()
    .setPostgres(true)
    .addLiquibaseRun("sql-test.xml", Map.of())
    .addConfiguration(List.of())
    .setConfigurationPath("config.json");

  private final Logger LOGGER = LoggerFactory.getLogger(RepositoryTests.class);


  @BeforeAll
  static void prepare() throws Exception {
    BOOTSTRAP.bootstrap();
  }


  @Test
  void insert(Vertx vertx, VertxTestContext vertxTestContext) {
    final var repository = new Repository<>(new TestModelMapper(), BOOTSTRAP.REPOSITORY_HANDLER);
    repository.insert(testModel()).await().indefinitely();
    vertxTestContext.completeNow();
  }

  @Test
  void insert_select(Vertx vertx, VertxTestContext vertxTestContext) {
    final var repository = new Repository<>(new TestModelMapper(), BOOTSTRAP.REPOSITORY_HANDLER);
    final var model = testModel();
    repository.insert(model).await().indefinitely();
    final var selectedValue = repository.selectByKey(new TestModelKey(model.textField())).await().indefinitely();
    LOGGER.info("Model time ->" + model.timeStampField());
    LOGGER.info("DB time ->" + selectedValue.timeStampField());
    assertTrue(model.timeStampField().isEqual(selectedValue.timeStampField()));
    vertxTestContext.completeNow();
  }

  @NotNull
  private static TestModel testModel() {
    return new TestModel(
      UUID.randomUUID().toString(),
      OffsetDateTime.now(),
      new JsonObject().put("somefield", "text"),
      0L,
      1,
      RecordWithoutID.newRecord()
    );
  }

  @AfterAll
  static void destroy() throws Exception {
    BOOTSTRAP.destroy();
  }
}
