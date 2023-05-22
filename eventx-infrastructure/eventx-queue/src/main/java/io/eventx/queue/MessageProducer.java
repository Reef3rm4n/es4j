package io.eventx.queue;

import io.eventx.queue.models.Message;
import io.eventx.queue.models.MessageID;
import io.eventx.queue.models.QueueTransaction;
import io.smallrye.mutiny.Uni;

import java.util.List;

public interface MessageProducer {
  <T> Uni<Void> enqueue(Message<T> message, QueueTransaction transaction);
  <T> Uni<Void> enqueue(List<Message<T>> entries, QueueTransaction queueTransaction);
  Uni<Void> cancel(MessageID messageID);
}
