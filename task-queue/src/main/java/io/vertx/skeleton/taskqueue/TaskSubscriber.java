package io.vertx.skeleton.taskqueue;


import io.smallrye.mutiny.Uni;
import io.vertx.skeleton.taskqueue.models.TaskProcessorManager;

public interface TaskSubscriber {

  Uni<Void> unsubscribe();

  Uni<Void> subscribe(TaskProcessorManager taskProcessorManager);



}
