package io.vertx.skeleton.ccp;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.VertxServiceException;

public class QueueException extends VertxServiceException {

  public QueueException(Error error) {
    super(error);
  }

}
