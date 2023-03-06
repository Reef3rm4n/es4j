package io.vertx.eventx.config;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventxException;

public class ConfigurationError extends EventxException {
  public ConfigurationError(EventXError eventxError) {
    super(eventxError);
  }

  public static EventxException illegalState() {
    return new EventxException(new EventXError("Illegal state", "", 500));
  }



}
