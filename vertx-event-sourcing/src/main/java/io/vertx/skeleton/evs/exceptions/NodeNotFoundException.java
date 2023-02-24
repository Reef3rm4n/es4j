package io.vertx.skeleton.evs.exceptions;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class NodeNotFoundException extends VertxServiceException {

  public NodeNotFoundException(Error error) {
    super(error);
  }
  public NodeNotFoundException(String entityId) {
    super(new Error("Handler not found for entityId" + entityId,"Cluster is most likely not ready to accept orders or handlers have not registered in the proxy hashRing",400));
  }
}
