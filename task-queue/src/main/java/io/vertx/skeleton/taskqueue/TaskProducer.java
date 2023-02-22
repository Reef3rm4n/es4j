package io.vertx.skeleton.taskqueue;

import io.smallrye.mutiny.Uni;
import io.vertx.skeleton.taskqueue.models.Message;
import io.vertx.skeleton.taskqueue.models.MessageID;
import io.vertx.skeleton.taskqueue.models.TaskTransaction;

import java.util.List;

public interface TaskProducer {
  <T> Uni<Void> enqueue(Message<T> message, TaskTransaction transaction);
  <T> Uni<Void> enqueue(List<Message<T>> entries, TaskTransaction taskTransaction);
  Uni<Void> cancel(MessageID messageID);
}
