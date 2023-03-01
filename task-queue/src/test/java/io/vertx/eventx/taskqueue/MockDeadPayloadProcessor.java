package io.vertx.eventx.taskqueue;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.queue.TaskProcessor;
import io.vertx.eventx.queue.models.TaskTransaction;

public class MockDeadPayloadProcessor implements TaskProcessor<MockDeadPayload> {
  @Override
  public Uni<Void> process(MockDeadPayload payload, TaskTransaction taskTransaction) {
    return Uni.createFrom().failure(new RuntimeException("Mocking failure !"));
  }
}
