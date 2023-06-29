package io.eventx.infrastructure.models;


import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Instant;
import java.util.List;

@RecordBuilder
public record ProjectionStream(
  String projectionId,
  List<String> events,
  List<String> aggregateIds,
  List<String> tags,
  String tenantId,
  Long offset,
  Integer batchSize,
  Instant from,
  Instant to,
  Long versionFrom,
  Long versionTo
) {
}
