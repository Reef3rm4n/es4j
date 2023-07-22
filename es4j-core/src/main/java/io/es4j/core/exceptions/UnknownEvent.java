package io.es4j.core.exceptions;


import io.es4j.Event;
import io.es4j.core.objects.Es4jError;

public class UnknownEvent extends Es4jException {
  public UnknownEvent(final Es4jError es4jError) {
    super(es4jError);
  }


  public static UnknownEvent unknown(Event event) {
    return new UnknownEvent(new Es4jError("Event Behaviour not found", event.getClass().getSimpleName() + " has not behaviour bind %s".formatted(event), 400));
  }

}
