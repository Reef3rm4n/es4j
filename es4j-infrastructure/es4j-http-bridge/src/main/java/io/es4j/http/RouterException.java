package io.es4j.http;


import io.es4j.core.objects.Es4jError;
import io.es4j.core.exceptions.Es4jException;

public class RouterException extends Es4jException {
  public RouterException(Es4jError es4jError) {
    super(es4jError);
  }

  public RouterException(Throwable throwable) {
    super(throwable);
  }

  public RouterException(Es4jError es4jError, Throwable throwable) {
    super(es4jError, throwable);
  }

  public RouterException(String cause, String hint, Integer errorCode) {
    super(cause, hint, errorCode);
  }

  public RouterException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, hint, errorCode, throwable);
  }
}
