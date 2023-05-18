package io.vertx.eventx.infrastructure.models;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Event;

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
