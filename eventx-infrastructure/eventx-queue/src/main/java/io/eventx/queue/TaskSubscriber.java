package io.eventx.queue;


import io.eventx.queue.models.MessageProcessorManager;
import io.smallrye.mutiny.Uni;

public interface TaskSubscriber {

  Uni<Void> unsubscribe();

  Uni<Void> subscribe(MessageProcessorManager messageProcessorManager);



}
