package io.es4j.infrastructure;

import io.es4j.Aggregate;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

public interface AggregateServices {
  Uni<Void> start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration);

  Uni<Void> stop();
}
