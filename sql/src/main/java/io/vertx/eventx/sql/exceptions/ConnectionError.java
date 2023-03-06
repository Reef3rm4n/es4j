package io.vertx.eventx.sql.exceptions;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventxException;

public class ConnectionError extends EventxException {

  public ConnectionError(EventXError cobraEventXError) {
    super(cobraEventXError);
  }

}
