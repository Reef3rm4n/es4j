package io.vertx.eventx;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.common.CommandHeaders;

import java.util.function.Consumer;

public interface Proxy<T extends Aggregate> {

  Uni<T> load(String entityId, CommandHeaders commandHeaders);
  <C extends Command> Uni<T> forward(C command);
  default Uni<Void> subscribe(Consumer<T> consumer) {
    return null;
  }

}
