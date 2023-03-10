package io.vertx.eventx.config;


import io.vertx.eventx.exceptions.EventxException;
import io.vertx.eventx.objects.EventxError;

public class ConfigurationError extends EventxException {
  public ConfigurationError(EventxError eventxError) {
    super(eventxError);
  }

  public static EventxException illegalState() {
    return new EventxException(new EventxError("Illegal state", "", 500));
  }



}
