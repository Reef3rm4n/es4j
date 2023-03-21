package io.vertx.eventx.queue.exceptions;

import io.vertx.core.json.JsonObject;

public class QueueException extends RuntimeException{
  private final QueueError eventxError;

  public QueueException(QueueError eventxError) {
    super(JsonObject.mapFrom(eventxError).encodePrettily());
    this.eventxError = eventxError;
  }

  public QueueException(Throwable throwable) {
    super(throwable.getMessage(), throwable);
    this.eventxError = new QueueError(
      throwable.getMessage(),
      throwable.getLocalizedMessage(),
      500
    );
  }
  public QueueException(QueueError eventxError, Throwable throwable) {
    super(eventxError.cause(), throwable);
    this.eventxError = eventxError;
  }

  public QueueException(String cause, String hint, Integer errorCode) {
    this.eventxError = new QueueError(cause, hint, errorCode);
  }

  public QueueException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, throwable);
    this.eventxError = new QueueError(cause, hint, errorCode);
  }

  public QueueError error() {
    return eventxError;
  }

}
