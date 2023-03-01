package io.vertx.eventx.sql.exceptions;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

public class ConnectionError extends EventXException {

  public ConnectionError(EventXError cobraEventXError) {
    super(cobraEventXError);
  }

}
