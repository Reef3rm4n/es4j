package io.vertx.eventx.core.exceptions;


import io.vertx.eventx.core.objects.EventxError;

public class NodeUnavailable extends EventxException {

  public NodeUnavailable(EventxError eventxError) {
    super(eventxError);
  }

  public NodeUnavailable(String entityId) {
    super(
      new EventxError(
        "Entity not found [ aggregateId:" + entityId + "]",
        "Entity not present in cluster",
        400
      )
    );
  }
}
