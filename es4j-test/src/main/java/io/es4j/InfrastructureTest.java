package io.es4j;

import io.es4j.infrastructure.EventStore;

public class InfrastructureTest {

  private final EventStore eventStore;

  public InfrastructureTest(EventStore eventStore) {
    this.eventStore = eventStore;
  }
}
