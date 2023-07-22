package io.es4j.infrastructure.messagebroker;

import com.google.auto.service.AutoService;
import io.es4j.infrastructure.pgbroker.QueueConsumer;
import io.es4j.infrastructure.pgbroker.models.ConsumerTransaction;
import io.smallrye.mutiny.Uni;

@AutoService(QueueConsumer.class)
public class MockPayloadConsumer implements QueueConsumer<MockPayload> {

  @Override
  public Uni<Void> process(MockPayload payload, ConsumerTransaction queueTransaction) {
    return Uni.createFrom().voidItem();
  }

}
