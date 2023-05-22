package io.eventx.infrastructure.models;

import io.eventx.Aggregate;
import io.eventx.Event;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Instant;
import java.util.List;

@RecordBuilder
public record EventStream(
  List<Class<? extends Aggregate>> aggregates,
  List<Class<? extends Event>> events,
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
