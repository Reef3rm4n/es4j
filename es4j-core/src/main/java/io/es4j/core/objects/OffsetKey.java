package io.es4j.core.objects;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record OffsetKey(
  String consumer,
  String tenantId
) {
}
