package io.vertx.eventx.exceptions;

import io.vertx.eventx.common.EventxError;
import io.vertx.eventx.common.exceptions.EventxException;

public class NodeUnavailable extends EventxException {

  public NodeUnavailable(EventxError eventxError) {
    super(eventxError);
  }
  public NodeUnavailable(String entityId) {
    super(new EventxError("Entity not found [ aggregateId:" + entityId + "]","Entity not present in cluster",400));
  }
}
