package io.es4j.http;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.Status;
import io.vertx.mutiny.core.Vertx;

public interface HealthCheck {


  Uni<Void> start(Vertx vertx, JsonObject configuration);

  String name();

  Uni<Status> checkHealth();
}
