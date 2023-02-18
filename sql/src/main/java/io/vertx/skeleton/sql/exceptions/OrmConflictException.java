package io.vertx.skeleton.sql.exceptions;


import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class OrmConflictException extends VertxServiceException {
  public OrmConflictException(Error cobraError) {
    super(cobraError);
  }

  public static <T> OrmConflictException conflict(Class<T> tClass, T object) {
    return new OrmConflictException(new Error("Conflicting record " + tClass.getSimpleName(), "Check object for conflict :" + object, 409));
  }

}
