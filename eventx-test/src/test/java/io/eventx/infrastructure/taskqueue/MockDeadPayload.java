package io.eventx.infrastructure.taskqueue;

public record MockDeadPayload(
  String data,
  boolean fatal
) {
}
