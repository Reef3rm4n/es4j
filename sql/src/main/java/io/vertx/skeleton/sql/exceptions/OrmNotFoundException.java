package io.vertx.skeleton.sql.exceptions;


import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class OrmNotFoundException extends VertxServiceException {
  public OrmNotFoundException(Error cobraError) {
    super(cobraError);
  }

  public static <T> OrmNotFoundException notFound(Class<T> tClass) {
    return new OrmNotFoundException(new Error(tClass.getSimpleName() + " not found !", "Check your query for details", 400));
  }

  public static OrmNotFoundException notFound(String table, String query) {
    return new OrmNotFoundException(new Error("Not found !", "Query in table " + table + " produced 0 results query[" + query + "]", 400));
  }

}
