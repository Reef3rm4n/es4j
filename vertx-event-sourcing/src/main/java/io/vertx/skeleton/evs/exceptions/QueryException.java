package io.vertx.skeleton.evs.exceptions;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class QueryException extends VertxServiceException {

  public QueryException(final Error error) {
    super(error);
  }

  public static QueryException unknown(Class<?> eventClass) {
    return new QueryException(new Error("Query Behaviour not found", "Query not found -> " + eventClass.getSimpleName(), 400));
  }

}
