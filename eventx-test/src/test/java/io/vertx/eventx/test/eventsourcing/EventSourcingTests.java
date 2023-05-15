package io.vertx.eventx.test.eventsourcing;


import io.vertx.eventx.EventxTestBootstrapper;
import io.vertx.eventx.core.EventProjectionPoller;
import io.vertx.eventx.core.EventbusEventProjection;
import io.vertx.eventx.core.EventbusStateProjection;
import io.vertx.eventx.core.StateProjectionPoller;
import io.vertx.eventx.infra.pg.PgEventStore;
import io.vertx.eventx.infra.pg.PgOffsetStore;
import io.vertx.eventx.infra.pg.mappers.EventStoreMapper;
import io.vertx.eventx.infra.pg.mappers.JournalOffsetMapper;
import io.vertx.eventx.objects.CommandHeaders;
import io.vertx.eventx.objects.StateProjectionWrapper;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.test.eventsourcing.commands.ChangeData;
import io.vertx.eventx.test.eventsourcing.commands.CreateData;
import io.vertx.eventx.test.eventsourcing.domain.FakeAggregate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

import static io.vertx.eventx.EventxTestBootstrapper.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class EventSourcingTests {
  private final Logger LOGGER = LoggerFactory.getLogger(EventSourcingTests.class);

  public static final EventxTestBootstrapper<FakeAggregate> EVENTX_BOOTSTRAP = new EventxTestBootstrapper<>(FakeAggregate.class)
    .setPostgres(true);


  @BeforeAll
  static void start() {
    EVENTX_BOOTSTRAP.bootstrap();
  }

  @AfterAll
  static void destroy() throws Exception {
    EVENTX_BOOTSTRAP.destroy();
  }

  @Test
  void test_eventbus_bridge() {
    final var vertxTestContext = new VertxTestContext();
    final var createDataCommand = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    final var entity = EVENTX_BOOTSTRAP.eventBusPoxy.command(createDataCommand).await().indefinitely();
    final var changeData = new ChangeData(entity.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    assertNotNull(entity.state().data().get("key"), "data should have been created");
    final var entityAfterChange = EVENTX_BOOTSTRAP.eventBusPoxy.command(changeData).await().indefinitely();
    assertNotEquals(entity.state().data().get("key"), entityAfterChange.state().data().get("key"), "data should have been replaced");
    vertxTestContext.completeNow();
  }

  @Test
  void test_snapshotting() {
    final var vertxTestContext = new VertxTestContext();
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    final var entity = EVENTX_BOOTSTRAP.eventBusPoxy.command(newData).await().indefinitely();
    final var changeData = new ChangeData(entity.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    assertNotNull(entity.state().data().get("key"), "data should have been created");
    final var entityAfterChange = EVENTX_BOOTSTRAP.eventBusPoxy.command(changeData).await().indefinitely();
    assertNotEquals(entity.state().data().get("key"), entityAfterChange.state().data().get("key"), "data should have been replaced");
    vertxTestContext.completeNow();
  }

  @Test
  void test_eventbus_state_projection() throws InterruptedException {
    final var vertxTestContext = new VertxTestContext();
    final var checkpoint = vertxTestContext.checkpoint(1);
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    EVENTX_BOOTSTRAP.eventBusPoxy.eventSubscribe(
      fakeAggregateAggregateState -> {
        LOGGER.info("Incoming event {}", fakeAggregateAggregateState.toJson().encodePrettily());
        checkpoint.flag();
      }
    ).await().indefinitely();
    final var entity = EVENTX_BOOTSTRAP.eventBusPoxy.command(newData).await().indefinitely();
    final var poller = new StateProjectionPoller<>(
      FakeAggregate.class,
      new StateProjectionWrapper<>(
        new EventbusStateProjection<>(
          EventxTestBootstrapper.vertx
        ),
        FakeAggregate.class,
        LoggerFactory.getLogger(EventbusStateProjection.class)
      ),
      EVENTX_BOOTSTRAP.eventBusPoxy,
      new PgEventStore(new Repository<>(EventStoreMapper.INSTANCE, repositoryHandler)),
      new PgOffsetStore(new Repository<>(JournalOffsetMapper.INSTANCE, repositoryHandler))
    );
    poller.performTask().await().indefinitely();
    Thread.sleep(2000);
    vertxTestContext.completeNow();
  }

  @Test
  void test_eventbus_event_projection() throws InterruptedException {
    final var vertxTestContext = new VertxTestContext();
    final var checkpoint = vertxTestContext.checkpoint(1);
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    EVENTX_BOOTSTRAP.eventBusPoxy.eventSubscribe(
      fakeAggregateAggregateState -> {
        LOGGER.info("Incoming event {}", fakeAggregateAggregateState.toJson().encodePrettily());
        checkpoint.flag();
      }
    ).await().indefinitely();
    final var entity = EVENTX_BOOTSTRAP.eventBusPoxy.command(newData).await().indefinitely();
    final var poller = new EventProjectionPoller(
      new EventbusEventProjection(EventxTestBootstrapper.vertx, FakeAggregate.class),
      new PgEventStore(new Repository<>(EventStoreMapper.INSTANCE, repositoryHandler)),
      new PgOffsetStore(new Repository<>(JournalOffsetMapper.INSTANCE, repositoryHandler))
    );
    poller.performTask().await().indefinitely();
    Thread.sleep(2000);
    vertxTestContext.completeNow();
  }


  @Test
  void test_projection() {

  }

  @Test
  void test_() {

  }
}
