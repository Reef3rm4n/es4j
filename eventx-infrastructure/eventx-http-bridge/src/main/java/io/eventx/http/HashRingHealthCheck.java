package io.eventx.http;

import io.eventx.Aggregate;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.Status;

import java.util.Objects;

import static io.eventx.infrastructure.bus.AggregateBus.HASH_RING_MAP;

public class HashRingHealthCheck implements HealthCheck {

  private final Class<? extends Aggregate> aggregateClass;

  public HashRingHealthCheck(Class<? extends Aggregate> aggregateClass) {
    this.aggregateClass = aggregateClass;
  }

  @Override
  public String name() {
    return aggregateClass.getSimpleName() + "-bus";
  }

  // implement health check on aggregate bus
  @Override
  public Uni<Status> checkHealth() {
    final var hashRing = HASH_RING_MAP.get(aggregateClass);
    if (Objects.isNull(hashRing) || hashRing.getNodes().isEmpty()) {
      return Uni.createFrom().item(Status.KO());
    }
    final var jsonObject = new JsonObject();
    hashRing.getNodes().forEach((value) -> jsonObject.put(value.getKey(), value.hashCode()));
    return Uni.createFrom().item(Status.OK(
      jsonObject
    ));
  }
}
