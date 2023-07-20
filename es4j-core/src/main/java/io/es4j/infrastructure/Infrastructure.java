package io.es4j.infrastructure;

import io.es4j.Es4jDeployment;
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

  public Uni<Void> setup(Es4jDeployment es4jDeployment, Vertx vertx, JsonObject infrastructureConfiguration) {
    final var list = new ArrayList<Uni<Void>>();
    cache.ifPresent(cache -> list.add(cache.setup(es4jDeployment.aggregateClass(), es4jDeployment.aggregateConfiguration())));
    secondaryEventStore.ifPresent(secondaryEventStore -> list.add(secondaryEventStore.setup(es4jDeployment, vertx, infrastructureConfiguration)));
    list.add(eventStore.setup(es4jDeployment, vertx, infrastructureConfiguration));
    list.add(offsetStore.setup(es4jDeployment, vertx, infrastructureConfiguration));
    return Uni.join().all(list).andFailFast().replaceWithVoid();
  }


  public void start(Es4jDeployment es4jDeployment, Vertx vertx, JsonObject configuration) {
    eventStore.start(es4jDeployment, vertx, configuration);
    offsetStore.start(es4jDeployment, vertx, configuration);
    secondaryEventStore.ifPresent(ses -> ses.start(es4jDeployment, vertx, configuration));
  }
}
