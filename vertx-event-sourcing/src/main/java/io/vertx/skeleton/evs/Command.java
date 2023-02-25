package io.vertx.skeleton.evs;


import io.vertx.skeleton.models.CommandHeaders;

public interface Command {
  String entityId();
  CommandHeaders commandHeaders();

}
