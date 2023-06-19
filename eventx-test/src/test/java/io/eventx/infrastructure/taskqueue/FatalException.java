package io.eventx.infrastructure.taskqueue;

public class FatalException extends RuntimeException {

  public FatalException(String message) {
    super(message);
  }
}
