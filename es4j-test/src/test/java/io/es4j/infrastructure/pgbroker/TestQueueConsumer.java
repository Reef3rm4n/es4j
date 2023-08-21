package io.es4j.infrastructure.pgbroker;

import com.google.auto.service.AutoService;
import io.es4j.infrastructure.pgbroker.models.ConsumerTransaction;
import io.smallrye.mutiny.Uni;

@AutoService(QueueConsumer.class)
public class TestQueueConsumer implements QueueConsumer<TestQueuePayload> {


  @Override
  public Uni<Void> process(TestQueuePayload payload, ConsumerTransaction queueTransaction) {
    if (payload.fail()) {
      return Uni.createFrom().failure(new RetryException("mocking failure"));
    }
    return Uni.createFrom().voidItem();
  }

  @Override
  public String address() {
    return TestQueuePayload.class.getName();
  }

}
