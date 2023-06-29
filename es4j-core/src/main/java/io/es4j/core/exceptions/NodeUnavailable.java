package io.es4j.core.exceptions;


import io.es4j.core.objects.Es4jError;

public class NodeUnavailable extends Es4jException {

  public NodeUnavailable(Es4jError es4jError) {
    super(es4jError);
  }

  public NodeUnavailable(String entityId) {
    super(
      new Es4jError(
        "Entity not found [ aggregateId:" + entityId + "]",
        "Entity not present in cluster",
        400
      )
    );
  }
}
