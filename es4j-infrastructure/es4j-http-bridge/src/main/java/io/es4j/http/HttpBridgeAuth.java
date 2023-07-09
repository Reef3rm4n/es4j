package io.es4j.http;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.ext.web.RoutingContext;

import java.util.List;

public interface HttpBridgeAuth {
  List<String> extractRoles(RoutingContext routingContext);

  default List<String> tenant() {
    return List.of("default");
  }

}
