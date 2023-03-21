package io.vertx.eventx.test.taskqueue;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.queue.MessageProcessor;
import io.vertx.eventx.queue.models.QueueTransaction;


public class MockProcessor implements MessageProcessor<MockPayload> {

  @Override
  public Uni<Void> process(MockPayload payload, QueueTransaction queueTransaction) {
    return Uni.createFrom().voidItem();
  }
}
