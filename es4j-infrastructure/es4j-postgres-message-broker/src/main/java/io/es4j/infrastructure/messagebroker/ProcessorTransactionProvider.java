package io.es4j.infrastructure.messagebroker;


import io.es4j.infrastructure.messagebroker.models.Message;
import io.es4j.infrastructure.messagebroker.models.QueueTransaction;
import io.smallrye.mutiny.Uni;

import java.util.function.BiFunction;

public interface ProcessorTransactionProvider {

  <M> Uni<Void> transaction(Message<M> message, BiFunction<Message<M>, QueueTransaction, Uni<Void>> function);
}
