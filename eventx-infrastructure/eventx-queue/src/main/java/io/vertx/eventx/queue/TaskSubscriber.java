package io.vertx.eventx.queue;


import io.smallrye.mutiny.Uni;
import io.vertx.eventx.queue.models.TaskProcessorManager;

public interface TaskSubscriber {

  Uni<Void> unsubscribe();

  Uni<Void> subscribe(TaskProcessorManager taskProcessorManager);



}
