package io.vertx.eventx.queue;


import io.smallrye.mutiny.Uni;
import io.vertx.eventx.queue.models.MessageProcessorManager;

public interface TaskSubscriber {

  Uni<Void> unsubscribe();

  Uni<Void> subscribe(MessageProcessorManager messageProcessorManager);



}
