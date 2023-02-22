package io.vertx.skeleton.taskqueue.exceptions;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class TaskProcessorException extends VertxServiceException {

  public TaskProcessorException(Error error) {
    super(error);
  }

}
