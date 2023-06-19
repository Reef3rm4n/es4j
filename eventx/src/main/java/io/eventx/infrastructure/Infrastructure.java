package io.eventx.infrastructure;

import io.eventx.Aggregate;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.ArrayList;
import java.util.Optional;

public record Infrastructure(
  Optional<AggregateCache> cache,
  EventStore eventStore,
  Optional<SecondaryEventStore> secondaryEventStore,
  OffsetStore offsetStore
) {

  public Uni<Void> stop() {
    final var list = new ArrayList<Uni<Void>>();
    cache.ifPresent(cache -> list.add(cache.close()));
    secondaryEventStore.ifPresent(secondaryEventStore -> list.add(secondaryEventStore.close()));
    list.add(eventStore.close());
    list.add(offsetStore.close());
    return Uni.join().all(list).andFailFast().replaceWithVoid();
  }

  public Uni<Void> start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration) {
    final var list = new ArrayList<Uni<Void>>();
    cache.ifPresent(cache -> list.add(cache.start(aggregateClass)));
    secondaryEventStore.ifPresent(secondaryEventStore -> list.add(secondaryEventStore.start(aggregateClass, vertx, configuration)));
    list.add(eventStore.start(aggregateClass, vertx, configuration));
    list.add(offsetStore.start(aggregateClass, vertx, configuration));
    return Uni.join().all(list).andFailFast().replaceWithVoid();
  }


}
