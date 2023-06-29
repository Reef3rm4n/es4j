package io.es4j.queue.exceptions;

public record QueueError(
  String cause,
  String hint,
  Integer internalCode
) {
}
