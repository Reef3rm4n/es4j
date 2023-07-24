package io.es4j.infrastructure.pgbroker;

public class RetryException extends RuntimeException {

  public RetryException(String message) {
    super(message);
  }
}
