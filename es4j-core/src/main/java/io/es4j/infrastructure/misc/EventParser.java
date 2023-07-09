package io.es4j.infrastructure.misc;

import io.es4j.Event;
import io.es4j.core.CommandHandler;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventParser.class);

  public static <T extends Event> T getEvent(final String eventClazz, JsonObject event) {
    try {
      final var eventClass = Class.forName(eventClazz);
      return (T) event.mapTo(eventClass);
    } catch (Exception e) {
      LOGGER.error("Unable to parse event %s error => %s".formatted(event.encode(), e.getMessage()), e);
      throw new IllegalArgumentException(e);
    }
  }


}
