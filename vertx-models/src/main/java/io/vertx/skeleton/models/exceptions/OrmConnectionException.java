package io.vertx.skeleton.models.exceptions;

import io.vertx.skeleton.models.Error;

public class OrmConnectionException extends VertxServiceException {

  public OrmConnectionException(Error error) {
    super(error);
  }

}
