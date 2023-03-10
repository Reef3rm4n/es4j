package io.vertx.eventx.queue.exceptions;


import io.vertx.eventx.exceptions.EventxException;
import io.vertx.eventx.objects.EventxError;

public class TaskProcessorException extends EventxException {

  public TaskProcessorException(EventxError eventxError) {
    super(eventxError);
  }

}
