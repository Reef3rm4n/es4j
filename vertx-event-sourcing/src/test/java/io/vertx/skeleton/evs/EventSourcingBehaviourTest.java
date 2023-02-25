package io.vertx.skeleton.evs;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mutiny.core.Vertx;
import io.vertx.skeleton.evs.actors.ChannelProxy;
import io.vertx.skeleton.evs.domain.FakeEntity;
import io.vertx.skeleton.evs.domain.commands.ChangeData1;
import io.vertx.skeleton.models.CommandHeaders;
import io.vertx.skeleton.test.VertxTestBootstrap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.UUID;

@ExtendWith(VertxExtension.class)

public class EventSourcingBehaviourTest {

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
  void test_command_event_behaviour(Vertx vertx, VertxTestContext vertxTestContext) {
    final var proxy = new ChannelProxy<>(vertx, FakeEntity.class);
    final var entity = proxy.forwardCommand(new ChangeData1(UUID.randomUUID().toString(), "", CommandHeaders.defaultHeaders())).await().indefinitely();
    vertxTestContext.completeNow();
  }


  @Test
  void test_projection() {

  }

  @Test
  void test_() {

  }
}
