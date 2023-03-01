package io.vertx.eventx.exceptions;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

public class UnknownEvent extends EventXException {
  public UnknownEvent(final EventXError eventxError) {
    super(eventxError);
  }


  public static UnknownEvent unknown(Class<?> eventClass) {
    return new UnknownEvent(new EventXError("Event Behaviour not found", eventClass.getSimpleName() + " has not behaviour bind", 400));
  }
}
