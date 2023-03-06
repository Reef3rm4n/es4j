package io.vertx.eventx.sql.exceptions;


import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventxException;

import java.util.List;

public class DataError extends EventxException {

  public DataError(EventXError cobraEventXError) {
    super(cobraEventXError);
  }
  public static <T> DataError nonUnique(Class<T> tClass, List<T> results) {
    return new DataError(new EventXError("Non Unique result for " + tClass.getSimpleName(), "Check the select query, result is not unique",409));
  }

}
