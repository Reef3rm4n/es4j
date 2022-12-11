package io.vertx.skeleton.evs.exceptions;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.VertxServiceException;

public class UnknownQueryException extends VertxServiceException {

  public UnknownQueryException(final Error error) {
    super(error);
  }

  public static UnknownQueryException unknown(Class<?> eventClass) {
    return new UnknownQueryException(new Error("Query Behaviour not found", "Query not found -> " + eventClass.getSimpleName(), 400));
  }
}
