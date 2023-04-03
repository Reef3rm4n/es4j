package io.vertx.eventx.test.eventsourcing.events;

import io.vertx.eventx.test.eventsourcing.FakeAggregate;

public record FakeSnapshot(
  FakeAggregate aggregateState
) {
}
