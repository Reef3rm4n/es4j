package io.es4j.infrastructure;


import io.es4j.Deployment;
import io.es4j.domain.FakeAggregate;
import io.es4j.infra.pg.PgEventStore;
import io.es4j.infrastructure.models.AggregateEventStreamBuilder;
import io.es4j.infrastructure.models.AppendInstruction;
import io.es4j.infrastructure.models.Event;
import io.es4j.sql.misc.Constants;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class EventStoreTest {

  public static final String TENANT_ID = "default";
  public static final Deployment DEPLOYMENT = () -> FakeAggregate.class;
  private static PostgreSQLContainer POSTGRES_CONTAINER;
  private static final Vertx vertx = Vertx.vertx();
  private static final JsonObject CONFIGURATION = new JsonObject();

  private static final Network NETWORK = Network.newNetwork();

  @BeforeAll
  static void setup() {
    deployContainers();
  }

  @AfterAll
  static void stop() {
    destroyContainers();
  }

  @ParameterizedTest
  @MethodSource("eventStores")
  void append_ensure_uniqueness(EventStore eventStore) {
    eventStore.setup(DEPLOYMENT, vertx, CONFIGURATION).await().indefinitely();
    eventStore.start(DEPLOYMENT, vertx, CONFIGURATION);
    // Define eventTypes and append instructions
    final var aggregateId = UUID.randomUUID().toString();
    final var goodAppend = createAppendInstruction(aggregateId);
    final var conflictingAppend = createAppendInstruction(aggregateId);

    // Append the first event
    Assertions.assertDoesNotThrow(
      () -> eventStore.append(goodAppend).await().indefinitely()
    );
    // Append the second event and expect an exception to be thrown
    Assertions.assertThrows(
      RuntimeException.class,
      () -> eventStore.append(conflictingAppend).await().indefinitely());
  }

  @ParameterizedTest
  @MethodSource("eventStores")
  void append_and_fetch(EventStore eventStore) {
    eventStore.setup(DEPLOYMENT, vertx, CONFIGURATION).await().indefinitely();
    eventStore.start(DEPLOYMENT, vertx, CONFIGURATION);
    // Define eventTypes and append instructions
    final var aggregateId = UUID.randomUUID().toString();
    int numberOfEvents = 100;
    final var goodAppend = createAppendInstruction(aggregateId, numberOfEvents);
    // Append the first event
    Assertions.assertDoesNotThrow(
      () -> eventStore.append(goodAppend).await().indefinitely()
    );
    // Append the second event and expect an exception to be thrown
    final var events = Assertions.assertDoesNotThrow(
      () -> eventStore.fetch(AggregateEventStreamBuilder.builder()
        .aggregateId(aggregateId)
        .tenantId(TENANT_ID)
        .build()
      ).await().indefinitely()
    );
    Assertions.assertEquals(numberOfEvents, events.size());
  }

  @ParameterizedTest
  @MethodSource("eventStores")
  void append_and_stream(EventStore eventStore) {
    eventStore.setup(DEPLOYMENT, vertx, CONFIGURATION).await().indefinitely();
    eventStore.start(DEPLOYMENT, vertx, CONFIGURATION);
    // Define eventTypes and append instructions
    final var aggregateId = UUID.randomUUID().toString();
    int numberOfEvents = 100;
    final var goodAppend = createAppendInstruction(aggregateId, numberOfEvents);
    // Append the first event
    Assertions.assertDoesNotThrow(
      () -> eventStore.append(goodAppend).await().indefinitely()
    );
    // Append the second event and expect an exception to be thrown
    final var atomicInt = new AtomicInteger(0);
    Assertions.assertDoesNotThrow(
      () -> eventStore.stream(AggregateEventStreamBuilder.builder()
          .aggregateId(aggregateId)
          .tenantId(TENANT_ID)
          .build(),
        event -> atomicInt.incrementAndGet()
      ).await().indefinitely()
    );
    Assertions.assertEquals(numberOfEvents, atomicInt.get());
  }

  private static AppendInstruction<FakeAggregate> createAppendInstruction(String aggregateId, int numberOfEvents) {
    Assertions.assertTrue(numberOfEvents > 0);
    final var events = IntStream.range(0, numberOfEvents).mapToObj(i -> createEvent(aggregateId, (long) i)).toList();
    return new AppendInstruction<>(
      FakeAggregate.class, aggregateId, TENANT_ID, events);
  }

  private static AppendInstruction<FakeAggregate> createAppendInstruction(String aggregateId) {
    return createAppendInstruction(aggregateId, 1);
  }

  private static Event createEvent(String aggregateId, Long version) {
    return new Event(null, aggregateId, "test-event", version, new JsonObject(), TENANT_ID, "test-event", List.of(), 0);
  }

  // This method provides different implementations of EventStore
  // to be used as parameters in the test
  private static Stream<EventStore> eventStores() {
    Stream<EventStore> eventStoreStream = Stream.of(
      new PgEventStore()
      // Add more implementations as needed
    );
    return eventStoreStream;
  }


  private static void deployContainers() {
    POSTGRES_CONTAINER = new PostgreSQLContainer<>("postgres:latest")
      .withNetwork(NETWORK)
      .waitingFor(Wait.forListeningPort());
    POSTGRES_CONTAINER.start();
    CONFIGURATION.put(Constants.PG_HOST, POSTGRES_CONTAINER.getHost())
      .put(Constants.PG_PORT, POSTGRES_CONTAINER.getFirstMappedPort())
      .put(Constants.PG_USER, POSTGRES_CONTAINER.getUsername())
      .put(Constants.PG_PASSWORD, POSTGRES_CONTAINER.getPassword())
      .put(Constants.PG_DATABASE, POSTGRES_CONTAINER.getDatabaseName())
      .put(Constants.JDBC_URL, POSTGRES_CONTAINER.getJdbcUrl());
  }

  private static void destroyContainers() {
    vertx.closeAndAwait();
    POSTGRES_CONTAINER.stop();
    POSTGRES_CONTAINER.close();
  }
}
