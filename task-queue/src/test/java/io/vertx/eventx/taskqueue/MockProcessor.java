package io.vertx.eventx.taskqueue;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.queue.TaskProcessor;
import io.vertx.eventx.queue.models.TaskTransaction;

public class MockProcessor implements TaskProcessor<MockPayload> {
  @Override
  public Uni<Void> process(MockPayload payload, TaskTransaction taskTransaction) {
    return Uni.createFrom().voidItem();
  }
}
