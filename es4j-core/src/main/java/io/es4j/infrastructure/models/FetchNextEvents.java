package io.es4j.infrastructure.models;


import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;

@RecordBuilder
public record FetchNextEvents(
  String projectionId,
  List<String> events,
  List<String> aggregateIds,
  List<String> tags,
  String tenantId,
  Integer batchSize
) {
}
