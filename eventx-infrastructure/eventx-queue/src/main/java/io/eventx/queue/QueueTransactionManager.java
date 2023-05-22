package io.eventx.queue;


import io.eventx.queue.models.Message;
import io.eventx.queue.models.QueueTransaction;
import io.smallrye.mutiny.Uni;

import java.util.function.BiFunction;

public interface QueueTransactionManager {

  <M> Uni<Void> transaction(Message<M> message, BiFunction<Message<M>, QueueTransaction, Uni<Void>> function);
}
