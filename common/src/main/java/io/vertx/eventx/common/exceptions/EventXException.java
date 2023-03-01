package io.vertx.eventx.common.exceptions;


import io.vertx.core.json.JsonObject;
import io.vertx.eventx.common.EventXError;

public class EventXException extends RuntimeException {
  private final EventXError eventxError;

  public EventXException(EventXError eventxError) {
    super(JsonObject.mapFrom(eventxError).encodePrettily());
    this.eventxError = eventxError;
  }

  public EventXException(Throwable throwable) {
    super(throwable.getMessage(), throwable);
    this.eventxError = new EventXError(
      throwable.getMessage(),
      throwable.getLocalizedMessage(),
      500
    );
  }
  public EventXException(EventXError eventxError, Throwable throwable) {
    super(eventxError.cause(), throwable);
    this.eventxError = eventxError;
  }

  public EventXException(String cause, String hint, Integer errorCode) {
    this.eventxError = new EventXError(cause, hint, errorCode);
  }

  public EventXException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, throwable);
    this.eventxError = new EventXError(cause, hint, errorCode);
  }

  public EventXError error() {
    return eventxError;
  }

}
