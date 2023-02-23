package io.vertx.skeleton.evs;


import io.vertx.skeleton.models.RequestHeaders;

public interface Command {
  String entityId();
  RequestHeaders requestHeaders();

}
