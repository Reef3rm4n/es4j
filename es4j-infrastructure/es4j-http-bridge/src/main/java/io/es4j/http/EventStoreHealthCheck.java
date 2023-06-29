package io.es4j.http;


import com.google.auto.service.AutoService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.Status;
import io.vertx.mutiny.core.Vertx;

@AutoService(HealthCheck.class)
public class EventStoreHealthCheck implements HealthCheck{
  @Override
  public Uni<Void> start(Vertx vertx, JsonObject configuration) {
    return Uni.createFrom().voidItem();
  }

  @Override
  public String name() {
    return "event-store-health";
  }

  @Override
  public Uni<Status> checkHealth() {
    return Uni.createFrom().item(Status.OK());
  }
  // todo ping the event-store
}
