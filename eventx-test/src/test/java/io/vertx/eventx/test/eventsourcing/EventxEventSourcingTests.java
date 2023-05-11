package io.vertx.eventx.test.eventsourcing;

import io.vertx.eventx.VertxTestBootstrap;
import io.vertx.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.vertx.eventx.objects.CommandHeaders;
import io.vertx.eventx.test.eventsourcing.commands.ChangeData;
import io.vertx.eventx.test.eventsourcing.commands.CreateData;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mutiny.core.Vertx;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class EventxEventSourcingTests {

  public static final VertxTestBootstrap BOOTSTRAP = new VertxTestBootstrap()
    .setPostgres(true)
    .setConfigurationPath("fakeaggregate.json");

  @BeforeAll
  static void prepare() throws Exception {
    BOOTSTRAP.bootstrap();
  }

  @AfterAll
  static void destroy() throws Exception {
    BOOTSTRAP.destroy();
  }

  @Test
  void test_eventbus_bridge(Vertx vertx, VertxTestContext vertxTestContext) {
    final var proxy = new AggregateEventBusPoxy<>(VertxTestBootstrap.VERTX, FakeAggregate.class);
    final var createDataCommand = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    final var entity = proxy.command(createDataCommand).await().indefinitely();
    final var changeData = new ChangeData(entity.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    assertNotNull(entity.state().data().get("key"), "data should have been created");
    final var entityAfterChange = proxy.command(changeData).await().indefinitely();
    assertNotEquals(entity.state().data().get("key"), entityAfterChange.state().data().get("key"), "data should have been replaced");
    vertxTestContext.completeNow();
  }

  @Test
  void test_snapshotting(Vertx vertx, VertxTestContext vertxTestContext) {
    final var proxy = new AggregateEventBusPoxy<>(VertxTestBootstrap.VERTX, FakeAggregate.class);
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    final var entity = proxy.command(newData).await().indefinitely();
    final var changeData = new ChangeData(entity.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    assertNotNull(entity.state().data().get("key"), "data should have been created");
    final var entityAfterChange = proxy.command(changeData).await().indefinitely();
    assertNotEquals(entity.state().data().get("key"), entityAfterChange.state().data().get("key"), "data should have been replaced");
    vertxTestContext.completeNow();
  }

  @Test
  void test_web_socket_proxy(Vertx vertx, VertxTestContext vertxTestContext) {
    final var proxy = new AggregateEventBusPoxy<>(BOOTSTRAP.VERTX, FakeAggregate.class);
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    final var entity = proxy.command(newData).await().indefinitely();
    final var changeData = new ChangeData(entity.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    assertNotNull(entity.state().data().get("key"), "data should have been created");
    final var entityAfterChange = proxy.command(changeData).await().indefinitely();
    assertNotEquals(entity.state().data().get("key"), entityAfterChange.state().data().get("key"), "data should have been replaced");
    vertxTestContext.completeNow();
  }


  @Test
  void test_projection() {

  }

  @Test
  void test_() {

  }
}
