package io.eventx.queue.exceptions;

public record QueueError(
  String cause,
  String hint,
  Integer internalCode
) {
}
