package io.eventx.http;

import io.eventx.Command;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.ext.web.RoutingContext;

import java.util.List;

public interface CommandAuth {
  Uni<Void> validateCommand(Command command, RoutingContext routingContext);

  default List<String> tenant() {
    return List.of("default");
  }

}
