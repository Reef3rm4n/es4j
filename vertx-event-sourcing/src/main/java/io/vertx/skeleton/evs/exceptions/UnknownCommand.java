package io.vertx.skeleton.evs.exceptions;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class UnknownCommand extends VertxServiceException {


  public UnknownCommand(final Error error) {
    super(error);
  }

  public static UnknownCommand unknown(Class<?> eventClass) {
    return new UnknownCommand(new Error("Command Behaviour not found", "Behaviour not found for command -> " + eventClass.getSimpleName(), 400));
  }

}
