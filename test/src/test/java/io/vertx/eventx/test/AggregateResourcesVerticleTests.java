package io.vertx.eventx.test;

import io.vertx.eventx.VertxTestBootstrap;
import io.vertx.eventx.handlers.AggregateChannelProxy;
import io.vertx.eventx.test.domain.FakeAggregate;
import io.vertx.eventx.test.domain.commands.ChangeData;
import io.vertx.eventx.test.domain.commands.CreateData;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mutiny.core.Vertx;
import io.vertx.eventx.common.CommandHeaders;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class AggregateResourcesVerticleTests {

  public static final VertxTestBootstrap BOOTSTRAP = new VertxTestBootstrap()
    .setPostgres(true)
    .addLiquibaseRun("event-sourcing.xml", Map.of("schema", "postgres"))
    .setConfigurationPath("config.json");

  @BeforeAll
  static void prepare() throws Exception {
    BOOTSTRAP.bootstrap();
  }

  @AfterAll
  static void destroy() throws Exception {
    BOOTSTRAP.destroy();
  }

  @Test
  void test_channel_proxy(Vertx vertx, VertxTestContext vertxTestContext) throws InterruptedException {
    final var proxy = new AggregateChannelProxy<>(VertxTestBootstrap.VERTX, FakeAggregate.class);
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    final var entity = proxy.command(newData).await().indefinitely();
    final var changeData = new ChangeData(entity.entityId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    assertNotNull(entity.data().get("key"), "data should have been created");
    final var entityAfterChange = proxy.command(changeData).await().indefinitely();
    assertNotEquals(entity.data().get("key"), entityAfterChange.data().get("key"), "data should have been replaced");
    vertxTestContext.completeNow();
  }

  @Test
  void test_projection(Vertx vertx, VertxTestContext vertxTestContext) throws InterruptedException {
    final var proxy = new AggregateChannelProxy<>(VertxTestBootstrap.VERTX, FakeAggregate.class);
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    final var entity = proxy.command(newData).await().indefinitely();
    final var changeData = new ChangeData(entity.entityId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    assertNotNull(entity.data().get("key"), "data should have been created");
    final var entityAfterChange = proxy.command(changeData).await().indefinitely();
    assertNotEquals(entity.data().get("key"), entityAfterChange.data().get("key"), "data should have been replaced");
    vertxTestContext.completeNow();
  }

  @Test
  void test_web_socket_proxy(Vertx vertx, VertxTestContext vertxTestContext) throws InterruptedException {
    final var proxy = new AggregateChannelProxy<>(BOOTSTRAP.VERTX, FakeAggregate.class);
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    final var entity = proxy.command(newData).await().indefinitely();
    final var changeData = new ChangeData(entity.entityId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    assertNotNull(entity.data().get("key"), "data should have been created");
    final var entityAfterChange = proxy.command(changeData).await().indefinitely();
    assertNotEquals(entity.data().get("key"), entityAfterChange.data().get("key"), "data should have been replaced");
    vertxTestContext.completeNow();
  }


  @Test
  void test_projection() {

  }

  @Test
  void test_() {

  }
}
