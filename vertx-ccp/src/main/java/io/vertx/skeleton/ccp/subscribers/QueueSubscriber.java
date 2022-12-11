package io.vertx.skeleton.ccp.subscribers;

import io.vertx.skeleton.ccp.consumers.MessageConsumer;
import io.smallrye.mutiny.Uni;

public interface QueueSubscriber {

  Uni<Void> unsubscribe();
  void subscribe(MessageConsumer messageConsumer, String verticleId);
}
