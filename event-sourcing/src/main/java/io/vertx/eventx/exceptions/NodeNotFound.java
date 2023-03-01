package io.vertx.eventx.exceptions;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

public class NodeNotFound extends EventXException {

  public NodeNotFound(EventXError eventxError) {
    super(eventxError);
  }
  public NodeNotFound(String entityId) {
    super(new EventXError("Entity not found [ aggregateId:" + entityId + "]","Entity not present in cluster",400));
  }
}
