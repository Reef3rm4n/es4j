package io.eventx.infrastructure;

import io.eventx.Aggregate;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

public interface AggregateServices {
  Uni<Void> start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration);

  Uni<Void> stop();
}
