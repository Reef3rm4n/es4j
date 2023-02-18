package io.vertx.skeleton.models.exceptions;


import io.vertx.skeleton.models.Error;

public class OrmConflictException extends VertxServiceException {
  public OrmConflictException(Error error) {
    super(error);
  }

  public static <T> OrmConflictException conflict(Class<T> tClass, T object) {
    return new OrmConflictException(new Error("Conflicting record " + tClass.getSimpleName(), "Check object for conflict :" + object, 409));
  }
}
