package io.vertx.eventx.common.exceptions;


import io.vertx.core.json.JsonObject;
import io.vertx.eventx.common.EventxError;


public class EventxException extends RuntimeException {
  private final EventxError eventxError;

  public EventxException(EventxError eventxError) {
    super(JsonObject.mapFrom(eventxError).encodePrettily());
    this.eventxError = eventxError;
  }

  public EventxException(Throwable throwable) {
    super(throwable.getMessage(), throwable);
    this.eventxError = new EventxError(
      throwable.getMessage(),
      throwable.getLocalizedMessage(),
      500
    );
  }
  public EventxException(EventxError eventxError, Throwable throwable) {
    super(eventxError.cause(), throwable);
    this.eventxError = eventxError;
  }

  public EventxException(String cause, String hint, Integer errorCode) {
    this.eventxError = new EventxError(cause, hint, errorCode);
  }

  public EventxException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, throwable);
    this.eventxError = new EventxError(cause, hint, errorCode);
  }

  public EventxError error() {
    return eventxError;
  }

}
