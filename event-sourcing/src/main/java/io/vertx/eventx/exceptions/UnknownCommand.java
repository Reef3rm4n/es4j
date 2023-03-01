package io.vertx.eventx.exceptions;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

public class UnknownCommand extends EventXException {


  public UnknownCommand(final EventXError eventxError) {
    super(eventxError);
  }

  public static UnknownCommand unknown(Class<?> eventClass) {
    return new UnknownCommand(new EventXError("Command Behaviour not found", "Behaviour not found for command -> " + eventClass.getSimpleName(), 400));
  }

}
