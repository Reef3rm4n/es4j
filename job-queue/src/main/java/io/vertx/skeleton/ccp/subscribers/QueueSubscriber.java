package io.vertx.skeleton.ccp.subscribers;

import io.vertx.skeleton.ccp.consumers.MessageHandler;
import io.smallrye.mutiny.Uni;

public interface QueueSubscriber {

  Uni<Void> unsubscribe();
  void subscribe(MessageHandler messageHandler, String verticleId);
}
