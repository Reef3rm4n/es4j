package io.eventx.http;


import io.eventx.core.objects.EventxError;
import io.eventx.core.exceptions.EventxException;

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
