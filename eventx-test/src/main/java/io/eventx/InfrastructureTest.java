package io.eventx;

import io.eventx.infrastructure.EventStore;

public class InfrastructureTest {

  private final EventStore eventStore;

  public InfrastructureTest(EventStore eventStore) {
    this.eventStore = eventStore;
  }
}
