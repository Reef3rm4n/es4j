package io.eventx.infrastructure.models;


import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record ResetProjection(
  String projectionId,
  String tenantId,
  Long idOffset
) {
}
