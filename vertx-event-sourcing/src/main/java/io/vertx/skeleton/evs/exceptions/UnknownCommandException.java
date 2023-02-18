package io.vertx.skeleton.evs.exceptions;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class UnknownCommandException extends VertxServiceException {


  public UnknownCommandException(final Error error) {
    super(error);
  }

  public static UnknownCommandException unknown(Class<?> eventClass) {
    return new UnknownCommandException(new Error("Command Behaviour not found", "Behaviour not found for command -> " + eventClass.getSimpleName(), 400));
  }

}
