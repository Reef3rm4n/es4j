package io.vertx.skeleton.models;

public class OrmNotFoundException extends VertxServiceException {
  public OrmNotFoundException(Error error) {
    super(error);
  }

  public static <T> OrmNotFoundException notFound(Class<T> tClass) {
    return new OrmNotFoundException(new Error(tClass.getSimpleName() + " not found !", "Check your query for details", 400));
  }

  public static <T> OrmNotFoundException notFound(String table, String query) {
    return new OrmNotFoundException(new Error("Not found !", "Query in table " + table + " produced 0 results query[" + query + "]", 400));
  }

}
