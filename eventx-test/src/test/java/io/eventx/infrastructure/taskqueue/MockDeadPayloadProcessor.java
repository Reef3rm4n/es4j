package io.eventx.infrastructure.taskqueue;

import io.smallrye.mutiny.Uni;
import io.eventx.queue.MessageProcessor;
import io.eventx.queue.models.QueueTransaction;

public class MockDeadPayloadProcessor implements MessageProcessor<MockDeadPayload> {
  @Override
  public Uni<Void> process(MockDeadPayload payload, QueueTransaction queueTransaction) {
    return Uni.createFrom().failure(new RuntimeException("Mocking failure !"));
  }
}
