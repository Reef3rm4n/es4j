package io.vertx.eventx.queue.exceptions;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

public class TaskProcessorException extends EventXException {

  public TaskProcessorException(EventXError eventxError) {
    super(eventxError);
  }

}
