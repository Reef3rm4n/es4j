package io.es4j.core.exceptions;


import io.es4j.core.objects.Es4jError;

public class UnknownEvent extends Es4jException {
  public UnknownEvent(final Es4jError es4jError) {
    super(es4jError);
  }


  public static UnknownEvent unknown(Class<?> eventClass) {
    return new UnknownEvent(new Es4jError("Event Behaviour not found", eventClass.getSimpleName() + " has not behaviour bind", 400));
  }

}
