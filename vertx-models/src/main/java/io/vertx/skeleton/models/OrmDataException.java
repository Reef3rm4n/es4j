package io.vertx.skeleton.models;

import java.util.List;

public class OrmDataException extends VertxServiceException {

  public OrmDataException(Error error) {
    super(error);
  }
  public static <T> OrmDataException nonUnique(Class<T> tClass, List<T> results) {
    return new OrmDataException(new Error("Non Unique result for " + tClass.getSimpleName(), "Check the select query, result is not unique",409));
  }

}
