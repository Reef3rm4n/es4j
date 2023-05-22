package io.eventx.http;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.healthchecks.Status;

public class AggregateBusHealthCheck implements HealthCheck {
  @Override
  public String name() {
    return null;
  }

  @Override
  public Uni<Status> checkHealth() {
    return null;
  }
}
