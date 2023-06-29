package io.es4j.infrastructure.models;

import io.es4j.Aggregate;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;
@RecordBuilder
public record AppendInstruction<T extends Aggregate>(
  Class<T> aggregate,
  String aggregateId,
  String tenantId,
  List<Event> events
) {
}
