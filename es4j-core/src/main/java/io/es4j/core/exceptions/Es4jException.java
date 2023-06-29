package io.es4j.core.exceptions;


import io.es4j.core.objects.Es4jError;
import io.vertx.core.json.JsonObject;


public class Es4jException extends RuntimeException {
  private final Es4jError es4jError;

  public Es4jException(Es4jError es4jError) {
    super(JsonObject.mapFrom(es4jError).encodePrettily());
    this.es4jError = es4jError;
  }

  public Es4jException(Throwable throwable) {
    super(throwable.getMessage(), throwable);
    this.es4jError = new Es4jError(
      throwable.getMessage(),
      throwable.getLocalizedMessage(),
      500
    );
  }
  public Es4jException(Es4jError es4jError, Throwable throwable) {
    super(es4jError.cause(), throwable);
    this.es4jError = es4jError;
  }

  public Es4jException(String cause, String hint, Integer errorCode) {
    this.es4jError = new Es4jError(cause, hint, errorCode);
  }

  public Es4jException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, throwable);
    this.es4jError = new Es4jError(cause, hint, errorCode);
  }

  public Es4jError error() {
    return es4jError;
  }

}
