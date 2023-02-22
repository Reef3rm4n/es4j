package io.vertx.skeleton.taskqueue;

import io.smallrye.mutiny.Uni;
import io.vertx.skeleton.taskqueue.models.TaskTransaction;

public class MockProcessor implements TaskProcessor<MockPayload>{
  @Override
  public Uni<Void> process(MockPayload payload, TaskTransaction taskTransaction) {
    return Uni.createFrom().voidItem();
  }
}
