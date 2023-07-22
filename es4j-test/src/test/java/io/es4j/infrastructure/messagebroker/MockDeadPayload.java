package io.es4j.infrastructure.messagebroker;

public record MockDeadPayload(
  String data,
  boolean fatal
) {
}
