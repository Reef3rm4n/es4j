package io.vertx.eventx.infrastructure;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Aggregate;
import io.vertx.mutiny.core.Vertx;

public record Infrastructure(
  AggregateCache cache,
  EventStore eventStore,
  OffsetStore offsetStore
) {

  public Uni<Void> stop() {
    return Uni.join().all(cache.close(), eventStore.close(), offsetStore.close())
      .usingConcurrencyOf(1).andFailFast()
      .replaceWithVoid();
  }

  public Uni<Void> start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration) {
    return Uni.join().all(cache.start(aggregateClass), eventStore.start(aggregateClass, vertx, configuration), offsetStore.start(aggregateClass, vertx, configuration))
      .usingConcurrencyOf(1).andFailFast()
      .replaceWithVoid();
  }


}
