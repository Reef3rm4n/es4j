package io.vertx.eventx.sql.exceptions;


import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

import java.util.List;

public class DataError extends EventXException {

  public DataError(EventXError cobraEventXError) {
    super(cobraEventXError);
  }
  public static <T> DataError nonUnique(Class<T> tClass, List<T> results) {
    return new DataError(new EventXError("Non Unique result for " + tClass.getSimpleName(), "Check the select query, result is not unique",409));
  }

}
