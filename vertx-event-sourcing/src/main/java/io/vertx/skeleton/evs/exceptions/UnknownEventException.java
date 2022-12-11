package io.vertx.skeleton.evs.exceptions;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.VertxServiceException;

public class UnknownEventException extends VertxServiceException {
  public UnknownEventException(final Error error) {
    super(error);
  }


  public static UnknownEventException unknown(Class<?> eventClass) {
    return new UnknownEventException(new Error("Event Behaviour not found", eventClass.getSimpleName() + " has not behaviour bind", 400));
  }
}
