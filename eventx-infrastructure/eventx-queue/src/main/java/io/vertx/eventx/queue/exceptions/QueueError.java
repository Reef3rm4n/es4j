package io.vertx.eventx.queue.exceptions;

public record QueueError(
  String cause,
  String hint,
  Integer internalCode
) {
}
