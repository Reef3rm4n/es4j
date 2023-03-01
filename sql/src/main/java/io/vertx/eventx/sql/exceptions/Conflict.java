package io.vertx.eventx.sql.exceptions;


import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

public class Conflict extends EventXException {
  public Conflict(EventXError cobraEventXError) {
    super(cobraEventXError);
  }

  public static <T> Conflict conflict(Class<T> tClass, T object) {
    return new Conflict(new EventXError("Conflicting record " + tClass.getSimpleName(), "Check object for conflict :" + object, 409));
  }

}
