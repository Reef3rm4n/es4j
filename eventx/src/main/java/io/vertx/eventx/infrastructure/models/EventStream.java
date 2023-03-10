package io.vertx.eventx.infrastructure.models;

import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Event;

import java.util.List;

public record EventStream(
  List<Class<? extends Aggregate>> aggregates,
  List<Class<? extends Event>> events,
  List<String> aggregateIds,
  List<String> tags,
  String tenantId,

  Long offset,
  Integer batchSize
) {
}
