package io.vertx.eventx.http;


import io.vertx.eventx.exceptions.EventxException;
import io.vertx.eventx.objects.EventxError;

public class RouterException extends EventxException {
  public RouterException(EventxError eventxError) {
    super(eventxError);
  }

  public RouterException(Throwable throwable) {
    super(throwable);
  }

  public RouterException(EventxError eventxError, Throwable throwable) {
    super(eventxError, throwable);
  }

  public RouterException(String cause, String hint, Integer errorCode) {
    super(cause, hint, errorCode);
  }

  public RouterException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, hint, errorCode, throwable);
  }
}
