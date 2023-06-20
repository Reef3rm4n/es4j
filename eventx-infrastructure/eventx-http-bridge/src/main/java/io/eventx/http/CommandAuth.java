package io.eventx.http;

import io.eventx.Command;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.MultiMap;

import java.util.List;

public interface CommandAuth {
  Uni<Void> validateCommand(Command command, MultiMap headers);
  default List<String> tenant() {
    return List.of("default");
  }

}
