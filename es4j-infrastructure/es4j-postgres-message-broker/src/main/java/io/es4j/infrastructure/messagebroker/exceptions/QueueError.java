package io.es4j.infrastructure.messagebroker.exceptions;

public record QueueError(
  String cause,
  String hint,
  Integer internalCode
) {
}
