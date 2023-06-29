package io.es4j.infrastructure.taskqueue;

public class FatalException extends RuntimeException {

  public FatalException(String message) {
    super(message);
  }
}
