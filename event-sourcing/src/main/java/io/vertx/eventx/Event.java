package io.vertx.eventx;

public interface Event {

  default int schemaVersion() {
    return 0;
  }

}
