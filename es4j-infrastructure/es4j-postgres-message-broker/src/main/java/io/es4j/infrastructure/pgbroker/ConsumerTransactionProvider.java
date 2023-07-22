package io.es4j.infrastructure.pgbroker;


import io.es4j.infrastructure.pgbroker.models.Message;
import io.es4j.infrastructure.pgbroker.models.ConsumerTransaction;
import io.es4j.sql.RepositoryHandler;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.function.BiFunction;

public interface ConsumerTransactionProvider {

  void start(RepositoryHandler repositoryHandler);

  <M> Uni<Void> transaction(String processorClass, Message<M> message, BiFunction<Message<M>, ConsumerTransaction, Uni<Void>> function);
}
