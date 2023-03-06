package io.vertx.eventx.http;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventxException;

public class RouterException extends EventxException {
  public RouterException(EventXError eventxError) {
    super(eventxError);
  }

  public RouterException(Throwable throwable) {
    super(throwable);
  }

  public RouterException(EventXError eventxError, Throwable throwable) {
    super(eventxError, throwable);
  }

  public RouterException(String cause, String hint, Integer errorCode) {
    super(cause, hint, errorCode);
  }

  public RouterException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, hint, errorCode, throwable);
  }
}
