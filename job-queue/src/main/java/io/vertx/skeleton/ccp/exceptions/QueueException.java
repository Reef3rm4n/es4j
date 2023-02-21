package io.vertx.skeleton.ccp.exceptions;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class QueueException extends VertxServiceException {

  public QueueException(Error error) {
    super(error);
  }

}
