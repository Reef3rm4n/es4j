package io.es4j.infrastructure.pgbroker;

public record TestTopicPayload(
  String data,
  boolean fail
) {
}
