package io.vertx.eventx.sql.exceptions;


import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

public class IntegrityContraintViolation extends EventXException {

  public IntegrityContraintViolation(EventXError cobraEventXError) {
    super(cobraEventXError);
  }

  public static <T> IntegrityContraintViolation violation(Class<T> tClass, Object object) {
    return new IntegrityContraintViolation(new EventXError(tClass.getSimpleName() + " violated integrity", "Should be unique but found -> " + object, 409));
  }

}
