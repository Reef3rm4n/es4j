package io.vertx.skeleton.evs;


import io.vertx.skeleton.models.RequestMetadata;

public interface Command {
  String entityId();
  RequestMetadata requestMetadata();

}
