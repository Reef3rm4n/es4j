package io.vertx.skeleton.models;

public class OrmConnectionException extends VertxServiceException {

  public OrmConnectionException(Error error) {
    super(error);
  }

}
