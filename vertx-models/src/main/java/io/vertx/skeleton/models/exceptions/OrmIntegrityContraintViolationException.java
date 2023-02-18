package io.vertx.skeleton.models.exceptions;

import io.vertx.skeleton.models.Error;

public class OrmIntegrityContraintViolationException extends VertxServiceException {

  public OrmIntegrityContraintViolationException(Error error) {
    super(error);
  }

  public static <T> OrmIntegrityContraintViolationException violation(Class<T> tClass, Object object) {
    return new OrmIntegrityContraintViolationException(new Error(tClass.getSimpleName() + " violated integrity", "Should be unique but found -> " + object, 409));
  }

}
