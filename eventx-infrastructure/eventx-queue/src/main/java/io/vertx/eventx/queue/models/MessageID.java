package io.vertx.eventx.queue.models;


public record MessageID(
  String id,
  String tenant
) {
}
