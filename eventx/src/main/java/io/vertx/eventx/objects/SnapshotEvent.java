package io.vertx.eventx.objects;

import io.vertx.eventx.Aggregate;

public record SnapshotEvent<T extends Aggregate>(
  AggregateState<T> state
) {
}
