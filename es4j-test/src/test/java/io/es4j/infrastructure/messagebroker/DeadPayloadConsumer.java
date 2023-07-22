package io.es4j.infrastructure.messagebroker;


import com.google.auto.service.AutoService;
import io.es4j.infrastructure.pgbroker.QueueConsumer;
import io.es4j.infrastructure.pgbroker.models.ConsumerTransaction;
import io.smallrye.mutiny.Uni;

import java.util.List;


@AutoService(QueueConsumer.class)
public class DeadPayloadConsumer implements QueueConsumer<MockDeadPayload> {
  @Override
  public Uni<Void> process(MockDeadPayload payload, ConsumerTransaction queueTransaction) {
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
