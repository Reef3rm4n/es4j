package io.vertx.skeleton.evs;


import io.vertx.skeleton.models.RequestMetadata;

public interface EntityAggregateCommand {

  String entityId();
  RequestMetadata requestMetadata();

}
