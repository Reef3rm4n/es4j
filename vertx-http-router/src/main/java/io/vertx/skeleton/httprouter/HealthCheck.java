package io.vertx.skeleton.httprouter;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.healthchecks.Status;

public interface HealthCheck {

  String name();

  Uni<Status> checkHealth();
}
