package io.es4j.infrastructure.models;

public class ConcurrentAppend extends RuntimeException {

  public ConcurrentAppend(Throwable throwable) {
    super(throwable);
  }
}
