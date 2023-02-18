package io.vertx.skeleton.sql.exceptions;


import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class OrmIntegrityContraintViolationException extends VertxServiceException {

  public OrmIntegrityContraintViolationException(Error cobraError) {
    super(cobraError);
  }

  public static <T> OrmIntegrityContraintViolationException violation(Class<T> tClass, Object object) {
    return new OrmIntegrityContraintViolationException(new Error(tClass.getSimpleName() + " violated integrity", "Should be unique but found -> " + object, 409));
  }

}
