package io.vertx.skeleton.sql.exceptions;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class OrmConnectionException extends VertxServiceException {

  public OrmConnectionException(Error cobraError) {
    super(cobraError);
  }

}
