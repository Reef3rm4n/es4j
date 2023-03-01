package io.vertx.eventx.config;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

public class ConfigurationError extends EventXException {
  public ConfigurationError(EventXError eventxError) {
    super(eventxError);
  }

  public static EventXException illegalState() {
    return new EventXException(new EventXError("Illegal state", "", 500));
  }



}
