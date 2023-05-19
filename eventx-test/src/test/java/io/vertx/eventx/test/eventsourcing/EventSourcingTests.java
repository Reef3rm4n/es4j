package io.vertx.eventx.test.eventsourcing;


import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.UniHelper;
import io.vertx.eventx.EventxTestBootstrapper;
import io.vertx.eventx.config.DBConfig;
import io.vertx.eventx.core.tasks.EventProjectionPoller;
import io.vertx.eventx.core.projections.EventbusEventStream;
import io.vertx.eventx.infra.pg.PgEventStore;
import io.vertx.eventx.infra.pg.PgOffsetStore;
import io.vertx.eventx.infra.pg.mappers.EventStoreMapper;
import io.vertx.eventx.infra.pg.mappers.JournalOffsetMapper;
import io.vertx.eventx.core.objects.AggregateState;
import io.vertx.eventx.core.objects.CommandHeaders;
import io.vertx.eventx.core.objects.LoadAggregate;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.exceptions.IntegrityContraintViolation;
import io.vertx.eventx.test.eventsourcing.commands.ChangeData;
import io.vertx.eventx.test.eventsourcing.commands.ChangeDataWithConfig;
import io.vertx.eventx.test.eventsourcing.commands.ChangeDataWithDbConfig;
import io.vertx.eventx.test.eventsourcing.commands.CreateData;
import io.vertx.eventx.test.eventsourcing.domain.DataConfiguration;
import io.vertx.eventx.test.eventsourcing.domain.FakeAggregate;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static io.vertx.eventx.EventxTestBootstrapper.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class EventSourcingTests {
  private final Logger LOGGER = LoggerFactory.getLogger(EventSourcingTests.class);

  public static final EventxTestBootstrapper<FakeAggregate> EVENTX_BOOTSTRAP = new EventxTestBootstrapper<>(FakeAggregate.class)
    .setPostgres(false);


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
    final var aggregate = sendDummyCommandAndBlock();
    final var changeData = new ChangeData(aggregate.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    assertNotNull(aggregate.state().data().get("key"), "data should have been created");
    final var entityAfterChange = EVENTX_BOOTSTRAP.eventBusPoxy.forward(changeData).await().indefinitely();
    assertNotEquals(aggregate.state().data().get("key"), entityAfterChange.state().data().get("key"), "data should have been replaced");
  }

  @Test
  void test_replay_and_versioning() {
    final var aggregate = createAggregate();
    final var resultingState = issueCommands(aggregate, 9);
    assertEquals(10L, resultingState.currentVersion());
    final var replayedState = EVENTX_BOOTSTRAP.httpClient.forward(new LoadAggregate(
      aggregate.state().aggregateId(),
      5L,
      null,
      CommandHeaders.defaultHeaders()
    )).await().indefinitely();
    assertEquals(5L, replayedState.currentVersion());

    final var stateAfterReplayCommand = EVENTX_BOOTSTRAP.httpClient.forward(new LoadAggregate(
      aggregate.state().aggregateId(),
      null,
      null,
      CommandHeaders.defaultHeaders()
    )).await().indefinitely();
    assertEquals(10L, stateAfterReplayCommand.currentVersion());
  }

  @Test
  void test_fs_configuration() {
    final var aggregate = createAggregate();
    final var newState = EVENTX_BOOTSTRAP.httpClient.forward(new ChangeDataWithConfig(
      aggregate.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders()
    )).await().indefinitely();
  }

  @Test
  void test_db_configuration() {
    final var dataConfigurationDBConfig = new DBConfig<>(DataConfiguration.class, repositoryHandler);
    dataConfigurationDBConfig.add(new DataConfiguration(
      true,
      null,
      null
    ))
      .onFailure(IntegrityContraintViolation.class).recoverWithNull()
      .await().indefinitely();
    final var aggregate = createAggregate();
    final var newState = EVENTX_BOOTSTRAP.httpClient.forward(new ChangeDataWithDbConfig(
      aggregate.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders()
    )).await().indefinitely();
  }

  @Test
  void test_snapshotting() {
    // todo snapshot should increment version by 1 trough a system event
    // check for Snapshot.class in event-log
    // check for event version increased by 1

  }

  @Test
  void test_http_bridge() {
    final var vertxTestContext = new VertxTestContext();
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    final var entity = EVENTX_BOOTSTRAP.httpClient.forward(newData).await().indefinitely();
    final var changeData = new ChangeData(entity.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    assertNotNull(entity.state().data().get("key"), "data should have been created");
    final var entityAfterChange = EVENTX_BOOTSTRAP.httpClient.forward(changeData).await().indefinitely();
    assertNotEquals(entity.state().data().get("key"), entityAfterChange.state().data().get("key"), "data should have been replaced");
    vertxTestContext.completeNow();
  }

  @Test
  void test_projection_poller() {
    EVENTX_BOOTSTRAP.eventBusPoxy.eventSubscribe(fakeAggregateAggregateState -> LOGGER.info("Incoming event {}", fakeAggregateAggregateState.toJson().encodePrettily()))
      .await().indefinitely();
    final var aggregate = createAggregate();
    final var poller = new EventProjectionPoller(
      new EventbusEventStream(EventxTestBootstrapper.vertx, FakeAggregate.class),
      new PgEventStore(new Repository<>(EventStoreMapper.INSTANCE, repositoryHandler)),
      new PgOffsetStore(new Repository<>(JournalOffsetMapper.INSTANCE, repositoryHandler))
    );
    poller.performTask().await().indefinitely();
  }


  @Test
  @Timeout(999999999)
  void infinite_load_test_2(VertxTestContext vertxTestContext) {
    createAndIssueCommands(Duration.ofSeconds(1));
  }

  @Test
  @Timeout(999999999)
  void infinite_load_test_1(VertxTestContext vertxTestContext) {
    IntStream.range(0, 20).forEach(i -> createAndIssueCommands(Duration.ofMillis(50)));
  }

  private void createAndIssueCommands(Duration delay) {
    final var aggregate = createAggregate();
    Multi.createBy().repeating().uni(() -> sendDummyCommand(aggregate.state().aggregateId()))
      .withDelay(delay)
      .until(fakeAggregateAggregateState -> false)
      .collect().last()
      .replaceWithVoid()
      .subscribe()
      .with(UniHelper.NOOP);
  }

  private AggregateState<FakeAggregate> issueCommands(AggregateState<FakeAggregate> aggregate, int numberOfCommands) {
    final var atomicState = new AtomicReference<>(aggregate);
    IntStream.range(0, numberOfCommands).forEach(i -> atomicState.set(sendDummyCommand(aggregate.state().aggregateId()).await().indefinitely()));
    return atomicState.get();
  }

  private static AggregateState<FakeAggregate> createAggregate() {
    final var createData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    final var state = EVENTX_BOOTSTRAP.httpClient.forward(createData).await().indefinitely();
    assertEquals(1L, state.currentVersion());
    return state;
  }

  private static AggregateState<FakeAggregate> sendDummyCommandAndBlock() {
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    return EVENTX_BOOTSTRAP.eventBusPoxy.forward(newData).await().indefinitely();
  }

  private static Uni<AggregateState<FakeAggregate>> sendDummyCommand(String aggregateId) {
    final var changeData = new ChangeData(aggregateId, Map.of("key", UUID.randomUUID().toString()), CommandHeaders.defaultHeaders());
    return EVENTX_BOOTSTRAP.httpClient.forward(changeData);
  }

}
