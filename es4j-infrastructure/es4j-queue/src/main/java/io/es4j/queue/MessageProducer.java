package io.es4j.queue;

import io.es4j.queue.models.Message;
import io.es4j.queue.models.MessageID;
import io.es4j.queue.models.QueueTransaction;
import io.smallrye.mutiny.Uni;

import java.util.List;

public interface MessageProducer {
  <T> Uni<Void> enqueue(Message<T> message, QueueTransaction transaction);
  <T> Uni<Void> enqueue(List<Message<T>> entries, QueueTransaction queueTransaction);
  Uni<Void> cancel(MessageID messageID);
}
