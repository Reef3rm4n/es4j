package io.vertx.skeleton.httprouter;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class RouterException extends VertxServiceException {
  public RouterException(Error error) {
    super(error);
  }

  public RouterException(Throwable throwable) {
    super(throwable);
  }

  public RouterException(Error error, Throwable throwable) {
    super(error, throwable);
  }

  public RouterException(String cause, String hint, Integer errorCode) {
    super(cause, hint, errorCode);
  }

  public RouterException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, hint, errorCode, throwable);
  }
}
