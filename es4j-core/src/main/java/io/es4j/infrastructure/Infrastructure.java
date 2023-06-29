package io.es4j.infrastructure;

import io.es4j.Aggregate;
import io.es4j.core.objects.AggregateConfiguration;
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
    secondaryEventStore.ifPresent(secondaryEventStore -> list.add(secondaryEventStore.stop()));
    list.add(eventStore.stop());
    list.add(offsetStore.stop());
    return Uni.join().all(list).andFailFast().replaceWithVoid();
  }

  public Uni<Void> setup(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration) {
    final var list = new ArrayList<Uni<Void>>();
    cache.ifPresent(cache -> list.add(cache.setup(aggregateClass, configuration.getJsonObject("aggregate-configuration", new JsonObject()).mapTo(AggregateConfiguration.class))));
    secondaryEventStore.ifPresent(secondaryEventStore -> list.add(secondaryEventStore.setup(aggregateClass, vertx, configuration)));
    list.add(eventStore.setup(aggregateClass, vertx, configuration));
    list.add(offsetStore.setup(aggregateClass, vertx, configuration));
    return Uni.join().all(list).andFailFast().replaceWithVoid();
  }


  public void start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration) {
    eventStore.start(aggregateClass, vertx, configuration);
    offsetStore.start(aggregateClass, vertx, configuration);
    secondaryEventStore.ifPresent(ses -> ses.start(aggregateClass, vertx, configuration));
  }
}
