package io.eventx.infrastructure.taskqueue;

import io.eventx.queue.exceptions.QueueException;
import io.smallrye.mutiny.Uni;
import io.eventx.queue.MessageProcessor;
import io.eventx.queue.models.QueueTransaction;

import java.util.List;

public class MockDeadPayloadProcessor implements MessageProcessor<MockDeadPayload> {
  @Override
  public Uni<Void> process(MockDeadPayload payload, QueueTransaction queueTransaction) {
    if (payload.fatal()) {
      return Uni.createFrom().failure(new FatalException("Fatal failure !"));
    }
    return Uni.createFrom().failure(new RuntimeException("Mocking failure !"));
  }

  @Override
  public List<Class<? extends Throwable>> fatalExceptions() {
    return List.of(FatalException.class);
  }


}
