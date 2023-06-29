package io.es4j.infrastructure.misc;

import io.es4j.Event;
import io.vertx.core.json.JsonObject;

public class EventParser {



  public static <T extends Event> T getEvent(final String eventClazz, JsonObject event) {
    try {
      final var eventClass = Class.forName(eventClazz);
      return (T) event.mapTo(eventClass);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to cast event");
    }
  }



}
