package io.vertx.skeleton.evs;

import io.vertx.skeleton.models.RequestMetadata;

public interface EntityAggregateQuery {

  String entityId();
  RequestMetadata requestMetadata();
}
