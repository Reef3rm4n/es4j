package io.vertx.skeleton.evs.exceptions;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class NodeNotFoundException extends VertxServiceException {

  public NodeNotFoundException(Error error) {
    super(error);
  }
  public NodeNotFoundException(String entityId) {
    super(new Error("Entity not found [ entityId:" + entityId + "]","Entity not present in cluster",400));
  }
}
