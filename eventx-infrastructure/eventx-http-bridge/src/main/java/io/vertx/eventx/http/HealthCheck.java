package io.vertx.eventx.http;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.healthchecks.Status;

public interface HealthCheck {

  String name();

  Uni<Status> checkHealth();
}
