package io.es4j.infrastructure.models;

public class EventStoreExeception extends RuntimeException{
  public EventStoreExeception(Throwable t) {
    super(t);
  }
}
