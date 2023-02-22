package io.vertx.skeleton.taskqueue.models;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class MessageProcessorException extends VertxServiceException {

  public MessageProcessorException(Error error) {
    super(error);
  }

  public MessageProcessorException(Throwable throwable) {
    super(throwable);
  }

  public MessageProcessorException(Error error, Throwable throwable) {
    super(error, throwable);
  }

  public MessageProcessorException(String cause, String hint, Integer errorCode) {
    super(cause, hint, errorCode);
  }

  public MessageProcessorException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, hint, errorCode, throwable);
  }

}
