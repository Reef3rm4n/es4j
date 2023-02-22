package io.vertx.skeleton.taskqueue;

import io.smallrye.mutiny.Uni;
import io.vertx.skeleton.taskqueue.models.TaskTransaction;

public class MockDeadPayloadProcessor implements TaskProcessor<MockDeadPayload> {
  @Override
  public Uni<Void> process(MockDeadPayload payload, TaskTransaction taskTransaction) {
    return Uni.createFrom().failure(new RuntimeException("Mocking failure !"));
  }
}
