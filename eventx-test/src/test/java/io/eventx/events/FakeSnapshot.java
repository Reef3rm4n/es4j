package io.eventx.events;

import io.eventx.domain.FakeAggregate;

public record FakeSnapshot(
  FakeAggregate aggregateState
) {
}
