package io.vertx.eventx.infrastructure.models;

import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Event;

import java.util.List;


public record AggregateEventStream<T extends Aggregate>(
  Class<T> aggregate,
  String aggregateId,
  String tenantId,
  Long eventVersionOffset,
  Long journalOffset,
  List<Class<? extends Event>> startFrom
) {
}
