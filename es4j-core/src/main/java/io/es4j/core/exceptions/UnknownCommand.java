package io.es4j.core.exceptions;


import io.es4j.core.objects.Es4jError;

public class UnknownCommand extends Es4jException {


  public UnknownCommand(final Es4jError es4jError) {
    super(es4jError);
  }

  public static UnknownCommand unknown(Class<?> eventClass) {
    return new UnknownCommand(new Es4jError("Command Behaviour not found", "Behaviour not found for command -> " + eventClass.getSimpleName(), 400));
  }

}
