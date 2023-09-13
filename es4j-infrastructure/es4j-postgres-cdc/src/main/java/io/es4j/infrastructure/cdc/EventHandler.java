package io.es4j.infrastructure.cdc;

import io.es4j.infrastructure.cdc.models.Change;

public interface EventHandler {

  void start();

  String table();

  String schema();

  void consume(Change change);

}
