package io.es4j.infrastructure.pgbroker;

public record TestQueuePayload(
  String data,
  boolean fail
) {
}
