package io.es4j.http;

import com.google.auto.service.AutoService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.Status;
import io.vertx.mutiny.core.Vertx;

import java.util.Objects;

import static io.es4j.infrastructure.bus.AggregateBus.HASH_RING_MAP;


@AutoService(HealthCheck.class)
public class HashRingHealthCheck implements HealthCheck {


  @Override
  public Uni<Void> start(Vertx vertx, JsonObject configuration) {
    return Uni.createFrom().voidItem();
  }

  @Override
  public String name() {
    return "hash-ring-health";
  }

  // implement health check on aggregate bus
  @Override
  public Uni<Status> checkHealth() {
    final var jsonObject = new JsonObject();
    HASH_RING_MAP.forEach(
      (aClass, hashRing) -> {
        if (Objects.isNull(hashRing) || hashRing.getNodes().isEmpty()) {
        } else {
          hashRing.getNodes().forEach((value) -> jsonObject.put(value.getKey(), value.hashCode()));
        }

      }
    );

    return Uni.createFrom().item(Status.OK(
      jsonObject
    ));
  }
}
