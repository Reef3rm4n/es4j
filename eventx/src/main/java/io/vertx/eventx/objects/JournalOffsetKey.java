package io.vertx.eventx.objects;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record JournalOffsetKey(
  String consumer,
  String tenantId
) {
}
