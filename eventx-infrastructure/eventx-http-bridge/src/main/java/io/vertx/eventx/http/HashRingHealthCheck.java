package io.vertx.eventx.http;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.ext.healthchecks.Status;

import static io.vertx.eventx.infrastructure.bus.AggregateBus.HASH_RING_MAP;

public class HashRingHealthCheck implements HealthCheck{

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
    if (HASH_RING_MAP.isEmpty()) {
      return Uni.createFrom().item(Status.KO());
    }
    return Uni.createFrom().item(Status.OK());
  }
}
