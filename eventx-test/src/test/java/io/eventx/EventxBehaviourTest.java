package io.eventx;


import io.eventx.commands.ChangeData;
import io.eventx.commands.ChangeDataWithConfig;
import io.eventx.commands.ChangeDataWithDbConfig;
import io.eventx.commands.CreateData;
import io.eventx.core.objects.AggregateState;
import io.eventx.core.objects.CommandHeaders;
import io.eventx.core.projections.EventbusEventStream;
import io.eventx.core.tasks.EventProjectionPoller;
import io.eventx.domain.DataBusinessRule;
import io.eventx.domain.FakeAggregate;
import io.eventx.infra.pg.PgEventStore;
import io.eventx.infra.pg.PgOffsetStore;
import io.eventx.infra.pg.mappers.EventStoreMapper;
import io.eventx.infra.pg.mappers.JournalOffsetMapper;
import io.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.eventx.sql.Repository;
import io.eventx.sql.RepositoryHandler;
import io.smallrye.mutiny.Uni;
import io.eventx.core.objects.LoadAggregate;
import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

@EventxTest(aggregate = FakeAggregate.class)
public class EventxBehaviourTest {
  private final Logger LOGGER = LoggerFactory.getLogger(EventxBehaviourTest.class);

  @Test
  void test_snapshotting() {

  }

  @Test
  void test_caching() {

  }

  @Test
  @DisplayName("Test projection polling mechanism")
  void test_projection_poller(AggregateEventBusPoxy<FakeAggregate> eventBusPoxy, RepositoryHandler repositoryHandler) {
    eventBusPoxy.eventSubscribe(fakeAggregateAggregateState -> LOGGER.info("Incoming event {}", fakeAggregateAggregateState.toJson().encodePrettily()))
      .await().indefinitely();
    final var aggregate = createAggregate(eventBusPoxy);
    final var poller = new EventProjectionPoller(
      new EventbusEventStream(Bootstrapper.vertx, FakeAggregate.class),
      new PgEventStore(new Repository<>(EventStoreMapper.INSTANCE, repositoryHandler)),
      new PgOffsetStore(new Repository<>(JournalOffsetMapper.INSTANCE, repositoryHandler))
    );
    poller.performTask().await().indefinitely();
  }

  @Test
  @Order(1)
  void test_eventbus_bridge(AggregateEventBusPoxy<FakeAggregate> eventBusPoxy) {
    final var aggregate = sendDummyCommandAndBlock(eventBusPoxy);
    final var changeData = new ChangeData(aggregate.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    Assertions.assertNotNull(aggregate.state().data().get("key"), "data should have been created");
    final var entityAfterChange = eventBusPoxy.forward(changeData).await().indefinitely();
    Assertions.assertNotEquals(aggregate.state().data().get("key"), entityAfterChange.state().data().get("key"), "data should have been replaced");
  }

  @Test
  @Order(2)
  void test_http_bridge(AggregateHttpClient<FakeAggregate> proxy) {
    final var vertxTestContext = new VertxTestContext();
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    final var entity = proxy.forward(newData).await().indefinitely();
    final var changeData = new ChangeData(entity.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    Assertions.assertNotNull(entity.state().data().get("key"), "data should have been created");
    final var entityAfterChange = proxy.forward(changeData).await().indefinitely();
    Assertions.assertNotEquals(entity.state().data().get("key"), entityAfterChange.state().data().get("key"), "data should have been replaced");
    vertxTestContext.completeNow();
  }

  @Test
  @Order(3)
  void test_replay_and_versioning(AggregateEventBusPoxy<FakeAggregate> eventBusPoxy) {
    final var aggregate = createAggregate(eventBusPoxy);
    final var resultingState = issueCommands(aggregate, 9, eventBusPoxy);
    Assertions.assertEquals(10L, resultingState.currentVersion());
    final var replayedState = eventBusPoxy.forward(new LoadAggregate(
      aggregate.state().aggregateId(),
      5L,
      null,
      CommandHeaders.defaultHeaders()
    )).await().indefinitely();
    Assertions.assertEquals(5L, replayedState.currentVersion());

    final var stateAfterReplayCommand = eventBusPoxy.forward(new LoadAggregate(
      aggregate.state().aggregateId(),
      null,
      null,
      CommandHeaders.defaultHeaders()
    )).await().indefinitely();
    Assertions.assertEquals(10L, stateAfterReplayCommand.currentVersion());
  }

  @Test
  @Order(4)
  @FileBusinessRule(configurationClass = DataBusinessRule.class, fileName = "data-configuration.json")
  void test_fs_configuration(AggregateEventBusPoxy<FakeAggregate> eventBusPoxy) {
    final var aggregate = createAggregate(eventBusPoxy);
    final var newState = eventBusPoxy.forward(new ChangeDataWithConfig(
      aggregate.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders()
    )).await().indefinitely();
  }

  @Test
  @Order(5)
  @DatabaseBusinessRule(configurationClass = DataBusinessRule.class, fileName = "data-configuration.json")
  void test_db_configuration(AggregateHttpClient<FakeAggregate> proxy, AggregateEventBusPoxy<FakeAggregate> eventBusPoxy) {
    final var aggregate = createAggregate(eventBusPoxy);
    final var newState = proxy.forward(new ChangeDataWithDbConfig(
      aggregate.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders()
    )).await().indefinitely();
  }

  private AggregateState<FakeAggregate> issueCommands(AggregateState<FakeAggregate> aggregate, int numberOfCommands, AggregateEventBusPoxy<FakeAggregate> eventBusPoxy) {
    final var atomicState = new AtomicReference<>(aggregate);
    IntStream.range(0, numberOfCommands).forEach(i -> atomicState.set(sendDummyCommand(eventBusPoxy, aggregate.state().aggregateId()).await().indefinitely()));
    return atomicState.get();
  }

  private static AggregateState<FakeAggregate> createAggregate(AggregateEventBusPoxy<FakeAggregate> eventBusPoxy) {
    final var createData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    final var state = eventBusPoxy.forward(createData).await().indefinitely();
    Assertions.assertEquals(1L, state.currentVersion());
    return state;
  }

  private static AggregateState<FakeAggregate> sendDummyCommandAndBlock(AggregateEventBusPoxy<FakeAggregate> eventBusPoxy) {
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    return eventBusPoxy.forward(newData).await().indefinitely();
  }

  private static Uni<AggregateState<FakeAggregate>> sendDummyCommand(AggregateEventBusPoxy<FakeAggregate> proxy, String aggregateId) {
    final var changeData = new ChangeData(aggregateId, Map.of("key", UUID.randomUUID().toString()), CommandHeaders.defaultHeaders());
    return proxy.forward(changeData);
  }

}
