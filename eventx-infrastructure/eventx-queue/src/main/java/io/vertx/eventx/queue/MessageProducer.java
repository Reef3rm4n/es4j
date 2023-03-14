package io.vertx.eventx.queue;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.queue.models.Message;
import io.vertx.eventx.queue.models.MessageID;
import io.vertx.eventx.queue.models.QueueTransaction;

import java.util.List;

public interface MessageProducer {
  <T> Uni<Void> enqueue(Message<T> message, QueueTransaction transaction);
  <T> Uni<Void> enqueue(List<Message<T>> entries, QueueTransaction queueTransaction);
  Uni<Void> cancel(MessageID messageID);
}
