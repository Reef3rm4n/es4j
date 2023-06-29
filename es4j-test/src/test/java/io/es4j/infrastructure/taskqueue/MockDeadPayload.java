package io.es4j.infrastructure.taskqueue;

public record MockDeadPayload(
  String data,
  boolean fatal
) {
}
