package io.vertx.skeleton.sql.models;

public class CobraException extends RuntimeException {
  private final CobraError cobraError;

  public CobraException(CobraError cobraError) {
    super(cobraError.cause());
    this.cobraError = cobraError;
  }

  public CobraException(Throwable throwable) {
    super(throwable.getMessage(), throwable);
    this.cobraError = new CobraError(
      throwable.getMessage(),
      throwable.getLocalizedMessage(),
      500
    );
  }
  public CobraException(CobraError cobraError, Throwable throwable) {
    super(cobraError.cause(), throwable);
    this.cobraError = cobraError;
  }

  public CobraException(String cause, String hint, Integer errorCode) {
    this.cobraError = new CobraError(cause, hint, errorCode);
  }

  public CobraException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, throwable);
    this.cobraError = new CobraError(cause, hint, errorCode);
  }

  public CobraError error() {
    return cobraError;
  }

}
