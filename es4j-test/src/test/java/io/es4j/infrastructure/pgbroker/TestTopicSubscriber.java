package io.es4j.infrastructure.pgbroker;


import com.google.auto.service.AutoService;
import io.es4j.infrastructure.pgbroker.models.ConsumerTransaction;
import io.smallrye.mutiny.Uni;

import java.util.List;


@AutoService(TopicSubscription.class)
public class TestTopicSubscriber implements TopicSubscription<TestTopicPayload> {
  @Override
  public Uni<Void> process(TestTopicPayload payload, ConsumerTransaction queueTransaction) {
    if (payload.fail()) {
      return Uni.createFrom().failure(new RetryException("Fatal failure !"));
    }
    return Uni.createFrom().voidItem();
  }

  @Override
  public List<Class<? extends Throwable>> retryOn() {
    return List.of(RetryException.class);
  }

  @Override
  public String address() {
    return TestTopicPayload.class.getName();
  }


}
