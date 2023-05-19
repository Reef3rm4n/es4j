package io.vertx.eventx.http;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Command;
import io.vertx.mutiny.core.MultiMap;

import java.util.List;

public interface CommandAuth {
  Uni<Void> validateCommand(Command command, MultiMap headers);
  default List<String> tenantId() {
    return List.of("default");
  }

}
