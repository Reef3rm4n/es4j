package io.vertx.eventx.queue;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.queue.models.Message;
import io.vertx.eventx.queue.models.MessageID;
import io.vertx.eventx.queue.models.TaskTransaction;

import java.util.List;

public interface TaskProducer {
  <T> Uni<Void> enqueue(Message<T> message, TaskTransaction transaction);
  <T> Uni<Void> enqueue(List<Message<T>> entries, TaskTransaction taskTransaction);
  Uni<Void> cancel(MessageID messageID);
}
