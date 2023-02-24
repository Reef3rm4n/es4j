package io.vertx.skeleton.evs.exceptions;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class EventException extends VertxServiceException {
  public EventException(final Error error) {
    super(error);
  }


  public static EventException unknown(Class<?> eventClass) {
    return new EventException(new Error("Event Behaviour not found", eventClass.getSimpleName() + " has not behaviour bind", 400));
  }
}
