package io.es4j.events;

import io.es4j.domain.FakeAggregate;

public record FakeSnapshot(
  FakeAggregate aggregateState
) {
}
