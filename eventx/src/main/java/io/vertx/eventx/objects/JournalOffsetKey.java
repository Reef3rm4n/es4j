package io.vertx.eventx.objects;

public record JournalOffsetKey(
  String consumer,
  String tenantId
) {
}
