package io.vertx.eventx.queue;


import io.smallrye.mutiny.Uni;
import io.vertx.eventx.queue.models.Message;
import io.vertx.eventx.queue.models.TaskTransaction;

import java.util.function.BiFunction;

public interface TransactionManager {

  <M> Uni<Void> transaction(Message<M> message, BiFunction<Message<M>, TaskTransaction, Uni<Void>> function);
}
