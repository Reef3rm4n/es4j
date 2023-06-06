package io.eventx.infrastructure.taskqueue;

import io.smallrye.mutiny.Uni;
import io.eventx.queue.MessageProcessor;
import io.eventx.queue.models.QueueTransaction;


public class MockProcessor implements MessageProcessor<MockPayload> {

  @Override
  public Uni<Void> process(MockPayload payload, QueueTransaction queueTransaction) {
    return Uni.createFrom().voidItem();
  }
}
