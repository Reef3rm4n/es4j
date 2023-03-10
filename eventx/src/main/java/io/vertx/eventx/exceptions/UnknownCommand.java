package io.vertx.eventx.exceptions;


import io.vertx.eventx.objects.EventxError;

public class UnknownCommand extends EventxException {


  public UnknownCommand(final EventxError eventxError) {
    super(eventxError);
  }

  public static UnknownCommand unknown(Class<?> eventClass) {
    return new UnknownCommand(new EventxError("Command Behaviour not found", "Behaviour not found for command -> " + eventClass.getSimpleName(), 400));
  }

}
