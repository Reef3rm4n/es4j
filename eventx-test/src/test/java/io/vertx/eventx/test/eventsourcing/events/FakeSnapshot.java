package io.vertx.eventx.test.eventsourcing.events;

import io.vertx.eventx.test.eventsourcing.domain.FakeAggregate;

public record FakeSnapshot(
  FakeAggregate aggregateState
) {
}
