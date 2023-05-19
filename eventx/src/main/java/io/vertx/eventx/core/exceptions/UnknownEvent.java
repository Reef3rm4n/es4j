package io.vertx.eventx.core.exceptions;


import io.vertx.eventx.core.objects.EventxError;

public class UnknownEvent extends EventxException {
  public UnknownEvent(final EventxError eventxError) {
    super(eventxError);
  }


  public static UnknownEvent unknown(Class<?> eventClass) {
    return new UnknownEvent(new EventxError("Event Behaviour not found", eventClass.getSimpleName() + " has not behaviour bind", 400));
  }

}
