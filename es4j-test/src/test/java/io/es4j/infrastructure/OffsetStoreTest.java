package io.es4j.infrastructure;

import io.es4j.Deployment;
import io.es4j.core.objects.Offset;
import io.es4j.core.objects.OffsetBuilder;
import io.es4j.core.objects.OffsetKeyBuilder;
import io.es4j.domain.FakeAggregate;
import io.es4j.infra.pg.PgOffsetStore;
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

import java.util.stream.Stream;

class OffsetStoreTest {

  public static final String TENANT_ID = "default";
  private static PostgreSQLContainer POSTGRES_CONTAINER;
  private static final Deployment DEPLOYMENT = () -> FakeAggregate.class;
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
  @MethodSource("offsetStores")
  void test_offset_store(OffsetStore offsetStore) {
    offsetStore.setup(DEPLOYMENT, vertx, CONFIGURATION).await().indefinitely();
    offsetStore.start(DEPLOYMENT, vertx, CONFIGURATION);
    final var name = "fake-consumer";
    final var offset = Assertions.assertDoesNotThrow(
      () -> offsetStore.put(createOffset(name)).await().indefinitely()
    );

    final var result = Assertions.assertDoesNotThrow(
      () -> offsetStore.put(OffsetBuilder.builder(offset).eventVersionOffset(1L).idOffSet(10L).build()).await().indefinitely()
    );
    Assertions.assertEquals(10L, result.idOffSet());
    Assertions.assertEquals(1L, result.eventVersionOffset());

    final var result2 = Assertions.assertDoesNotThrow(
      () -> offsetStore.get(OffsetKeyBuilder.builder().consumer(name).tenantId(TENANT_ID).build()).await().indefinitely()
    );
    Assertions.assertEquals(10L, result2.idOffSet());
    Assertions.assertEquals(1L, result2.eventVersionOffset());
  }

  private static Offset createOffset(String name) {
    return OffsetBuilder.builder()
      .consumer(name)
      .tenantId(TENANT_ID)
      .idOffSet(0L)
      .eventVersionOffset(0L)
      .build();
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

  private static Stream<OffsetStore> offsetStores() {
    return Stream.of(
      new PgOffsetStore()
      // Add more implementations as needed
    );
  }

}
