package io.vertx.eventx.queue.exceptions;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventxException;

public class TaskProcessorException extends EventxException {

  public TaskProcessorException(EventXError eventxError) {
    super(eventxError);
  }

}
