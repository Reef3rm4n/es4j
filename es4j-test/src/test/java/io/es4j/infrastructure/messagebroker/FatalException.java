package io.es4j.infrastructure.messagebroker;

public class FatalException extends RuntimeException {

  public FatalException(String message) {
    super(message);
  }
}
