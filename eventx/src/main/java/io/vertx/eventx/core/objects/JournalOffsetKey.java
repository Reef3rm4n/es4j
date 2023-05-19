package io.vertx.eventx.core.objects;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record JournalOffsetKey(
  String consumer,
  String tenantId
) {
}
