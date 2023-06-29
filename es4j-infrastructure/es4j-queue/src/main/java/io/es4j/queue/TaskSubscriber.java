package io.es4j.queue;


import io.es4j.queue.models.MessageProcessorManager;
import io.smallrye.mutiny.Uni;

public interface TaskSubscriber {

  Uni<Void> unsubscribe();

  Uni<Void> subscribe(MessageProcessorManager messageProcessorManager);



}
