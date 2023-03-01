package io.vertx.eventx.exceptions;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

public class RestChannelError extends EventXException {
  public RestChannelError(EventXError eventxError) {
    super(eventxError);
  }
}
