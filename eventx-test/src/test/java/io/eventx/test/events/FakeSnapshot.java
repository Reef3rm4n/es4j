package io.eventx.test.events;

import io.eventx.test.domain.FakeAggregate;

public record FakeSnapshot(
  FakeAggregate aggregateState
) {
}
