package io.es4j.infrastructure.taskqueue;

import io.smallrye.mutiny.Uni;
import io.es4j.queue.MessageProcessor;
import io.es4j.infrastructure.messagebroker.models.QueueTransaction;


public class MockProcessor implements MessageProcessor<MockPayload> {

  @Override
  public Uni<Void> process(MockPayload payload, QueueTransaction queueTransaction) {
    return Uni.createFrom().voidItem();
  }
}
