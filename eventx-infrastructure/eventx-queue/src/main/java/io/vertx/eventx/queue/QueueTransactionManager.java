package io.vertx.eventx.queue;


import io.smallrye.mutiny.Uni;
import io.vertx.eventx.queue.models.Message;
import io.vertx.eventx.queue.models.QueueTransaction;

import java.util.function.BiFunction;

public interface QueueTransactionManager {

  <M> Uni<Void> transaction(Message<M> message, BiFunction<Message<M>, QueueTransaction, Uni<Void>> function);
}
