package io.vertx.skeleton.sql.exceptions;


import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

import java.util.List;

public class OrmDataException extends VertxServiceException {

  public OrmDataException(Error cobraError) {
    super(cobraError);
  }
  public static <T> OrmDataException nonUnique(Class<T> tClass, List<T> results) {
    return new OrmDataException(new Error("Non Unique result for " + tClass.getSimpleName(), "Check the select query, result is not unique",409));
  }

}
