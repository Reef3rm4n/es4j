package io.vertx.eventx.exceptions;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

public class QueryException extends EventXException {

  public QueryException(final EventXError eventxError) {
    super(eventxError);
  }

  public static QueryException unknown(Class<?> eventClass) {
    return new QueryException(new EventXError("Query Behaviour not found", "Query not found -> " + eventClass.getSimpleName(), 400));
  }

}
