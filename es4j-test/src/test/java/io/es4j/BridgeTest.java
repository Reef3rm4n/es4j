package io.es4j;


import io.es4j.commands.ChangeData;
import io.es4j.commands.ChangeDataWithConfig;
import io.es4j.commands.ChangeDataWithDbConfig;
import io.es4j.commands.CreateData;
import io.es4j.core.objects.AggregateState;
import io.es4j.core.objects.CommandHeaders;
import io.es4j.domain.DataBusinessRule;
import io.es4j.domain.FakeAggregate;
import io.es4j.infrastructure.proxy.AggregateEventBusPoxy;
import io.smallrye.mutiny.Uni;
import io.es4j.core.objects.LoadAggregate;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

@Es4jTest(aggregate = FakeAggregate.class)
public class BridgeTest {
  private final Logger LOGGER = LoggerFactory.getLogger(BridgeTest.class);

  @Test
  void test_snapshotting() {

  }

  @Test
  void test_caching() {

  }

//  @Test
//  @DisplayName("Test projection polling mechanism")
//  void test_projection_poller(AggregateEventBusPoxy<FakeAggregate> eventBusPoxy, RepositoryHandler repositoryHandler) {
//    eventBusPoxy.eventSubscribe(fakeAggregateAggregateState -> LOGGER.info("Incoming event {}", fakeAggregateAggregateState.toJson().encodePrettily()))
//      .await().indefinitely();
//    final var poller = new EventProjectionPoller(
//      events -> {
//        LOGGER.info("events {}", events);
//        return Uni.createFrom().voidItem();
//      },
//      new PgEventStore(),
//      new PgOffsetStore()
//    );
//    poller.performTask().await().indefinitely();
//  }

  @Test
  @Order(1)
  void test_eventbus_bridge(AggregateEventBusPoxy<FakeAggregate> eventBusPoxy) {
    final var aggregate = sendDummyCommandAndBlock(eventBusPoxy);
    final var changeData = new ChangeData(aggregate.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders());
    Assertions.assertNotNull(aggregate.state().data().get("key"), "data should have been created");
    final var entityAfterChange = eventBusPoxy.proxyCommand(changeData).await().indefinitely();
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
    final var replayedState = eventBusPoxy.proxyCommand(new LoadAggregate(
      aggregate.state().aggregateId(),
      "default",
      5L,
      null,
      CommandHeaders.defaultHeaders()
    )).await().indefinitely();
    Assertions.assertEquals(5L, replayedState.currentVersion());

    final var stateAfterReplayCommand = eventBusPoxy.proxyCommand(new LoadAggregate(
      aggregate.state().aggregateId(),
      "default",
      null,
      null,
      CommandHeaders.defaultHeaders()
    )).await().indefinitely();
    Assertions.assertEquals(10L, stateAfterReplayCommand.currentVersion());
  }

  @Test
  @Order(4)
  @FileBusinessRule(fileName = "data-configuration.json")
  void test_fs_configuration(AggregateEventBusPoxy<FakeAggregate> eventBusPoxy) {
    final var aggregate = createAggregate(eventBusPoxy);
    final var newState = eventBusPoxy.proxyCommand(new ChangeDataWithConfig(
      aggregate.state().aggregateId(), Map.of("key", "value2"), CommandHeaders.defaultHeaders()
    )).await().indefinitely();
  }

  @Test
  @Order(5)
  @DatabaseBusinessRule(configurationClass = DataBusinessRule.class, fileName = "data-configuration.json")
  void test_db_configuration(AggregateEventBusPoxy<FakeAggregate> proxy, AggregateEventBusPoxy<FakeAggregate> eventBusPoxy) {
    final var aggregate = createAggregate(eventBusPoxy);
    final var newState = proxy.proxyCommand(new ChangeDataWithDbConfig(
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
    final var state = eventBusPoxy.proxyCommand(createData).await().indefinitely();
    Assertions.assertEquals(1L, state.currentVersion());
    return state;
  }

  private static AggregateState<FakeAggregate> sendDummyCommandAndBlock(AggregateEventBusPoxy<FakeAggregate> eventBusPoxy) {
    final var newData = new CreateData(UUID.randomUUID().toString(), Map.of("key", "value"), CommandHeaders.defaultHeaders());
    return eventBusPoxy.proxyCommand(newData).await().indefinitely();
  }

  private static Uni<AggregateState<FakeAggregate>> sendDummyCommand(AggregateEventBusPoxy<FakeAggregate> proxy, String aggregateId) {
    final var changeData = new ChangeData(aggregateId, Map.of("key", UUID.randomUUID().toString()), CommandHeaders.defaultHeaders());
    return proxy.proxyCommand(changeData);
  }

}
