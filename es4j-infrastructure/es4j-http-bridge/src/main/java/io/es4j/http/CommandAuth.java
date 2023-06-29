package io.es4j.http;

import io.es4j.Command;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.ext.web.RoutingContext;

import java.util.List;

public interface CommandAuth {
  Uni<Void> validateCommand(Command command, RoutingContext routingContext);

  default List<String> tenant() {
    return List.of("default");
  }

}
