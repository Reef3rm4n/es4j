package io.vertx.skeleton.taskqueue;


import io.smallrye.mutiny.Uni;
import io.vertx.skeleton.taskqueue.models.Message;
import io.vertx.skeleton.taskqueue.models.TaskTransaction;

import java.util.function.BiFunction;

public interface TransactionManager {

  <M> Uni<Void> transaction(Message<M> message, BiFunction<Message<M>, TaskTransaction, Uni<Void>> function);
}
