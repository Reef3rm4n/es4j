package io.es4j.infrastructure;

import io.es4j.Aggregate;
import io.es4j.Deployment;
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

  public Uni<Void> setup(Deployment deployment, Vertx vertx, JsonObject infrastructureConfiguration) {
    final var list = new ArrayList<Uni<Void>>();
    cache.ifPresent(cache -> list.add(cache.setup(deployment.aggregateClass(), deployment.aggregateConfiguration())));
    secondaryEventStore.ifPresent(secondaryEventStore -> list.add(secondaryEventStore.setup(deployment, vertx, infrastructureConfiguration)));
    list.add(eventStore.setup(deployment, vertx, infrastructureConfiguration));
    list.add(offsetStore.setup(deployment, vertx, infrastructureConfiguration));
    return Uni.join().all(list).andFailFast().replaceWithVoid();
  }


  public void start(Deployment deployment, Vertx vertx, JsonObject configuration) {
    eventStore.start(deployment, vertx, configuration);
    offsetStore.start(deployment, vertx, configuration);
    secondaryEventStore.ifPresent(ses -> ses.start(deployment, vertx, configuration));
  }
}
