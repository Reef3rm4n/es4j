package io.es4j.infrastructure.models;

import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Instant;
import java.util.List;

@RecordBuilder
public record EventStream(
  List<String> eventTypes,
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
