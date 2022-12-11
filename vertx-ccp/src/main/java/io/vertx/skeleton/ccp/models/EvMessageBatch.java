package io.vertx.skeleton.ccp.models;

import java.util.List;

public record EvMessageBatch(
  List<EvMessage> messages
) implements java.io.Serializable {
}
