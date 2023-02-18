package io.vertx.skeleton.models.exceptions;


import io.vertx.skeleton.models.Error;

public class VertxServiceException extends RuntimeException {
  private final Error error;

  public VertxServiceException(Error error) {
    super(error.cause());
    this.error = error;
  }

  public VertxServiceException(Throwable throwable) {
    super(throwable.getMessage(), throwable);
    this.error = new Error(
      throwable.getMessage(),
      throwable.getLocalizedMessage(),
      500
    );
  }
  public VertxServiceException(Error error, Throwable throwable) {
    super(error.cause(), throwable);
    this.error = error;
  }

  public VertxServiceException(String cause, String hint, Integer errorCode) {
    this.error = new Error(cause, hint, errorCode);
  }

  public VertxServiceException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, throwable);
    this.error = new Error(cause, hint, errorCode);
  }

  public Error error() {
    return error;
  }

}
