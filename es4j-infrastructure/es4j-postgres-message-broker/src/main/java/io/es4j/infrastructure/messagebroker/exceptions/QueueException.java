package io.es4j.infrastructure.messagebroker.exceptions;

import io.vertx.core.json.JsonObject;

public class QueueException extends RuntimeException{
  private final QueueError queueError;

  public QueueException(QueueError queueError) {
    super(JsonObject.mapFrom(queueError).encodePrettily());
    this.queueError = queueError;
  }

  public QueueException(Throwable throwable) {
    super(throwable.getMessage(), throwable);
    this.queueError = new QueueError(
      throwable.getMessage(),
      throwable.getLocalizedMessage(),
      500
    );
  }
  public QueueException(QueueError queueError, Throwable throwable) {
    super(queueError.cause(), throwable);
    this.queueError = queueError;
  }

  public QueueException(String cause, String hint, Integer errorCode) {
    this.queueError = new QueueError(cause, hint, errorCode);
  }

  public QueueException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, throwable);
    this.queueError = new QueueError(cause, hint, errorCode);
  }

  public QueueError error() {
    return queueError;
  }

}
