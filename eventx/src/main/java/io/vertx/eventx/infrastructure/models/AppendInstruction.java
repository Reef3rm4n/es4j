package io.vertx.eventx.infrastructure.models;

import io.vertx.eventx.Aggregate;

import java.util.List;

public record AppendInstruction<T extends Aggregate>(
  Class<T> aggregate,
  String aggregateId,
  String tenantId,
  List<Event> events
) {
}
