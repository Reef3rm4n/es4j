package io.vertx.eventx.sql.exceptions;


import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventxException;

public class NotFound extends EventxException {
  public NotFound(EventXError cobraEventXError) {
    super(cobraEventXError);
  }

  public static <T> NotFound notFound(Class<T> tClass) {
    return new NotFound(new EventXError(tClass.getSimpleName() + " not found !", "Check your query for details", 400));
  }

  public static NotFound notFound(String table, String query) {
    return new NotFound(new EventXError("Not found !", "Query in table " + table + " produced 0 results query[" + query + "]", 400));
  }

}
