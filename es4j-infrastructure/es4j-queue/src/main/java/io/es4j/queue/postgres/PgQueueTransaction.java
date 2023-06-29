package io.es4j.queue.postgres;


import io.es4j.queue.postgres.mappers.MessageTransactionMapper;
import io.es4j.queue.postgres.models.MessageTransaction;
import io.es4j.queue.postgres.models.MessageTransactionID;
import io.es4j.queue.postgres.models.MessageTransactionQuery;
import io.es4j.sql.Repository;
import io.es4j.sql.RepositoryHandler;
import io.es4j.sql.models.BaseRecord;
import io.smallrye.mutiny.Uni;
import io.es4j.queue.QueueTransactionManager;
import io.es4j.queue.models.Message;
import io.es4j.queue.models.QueueTransaction;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.function.BiFunction;

public class PgQueueTransaction implements QueueTransactionManager {
  private final Repository<MessageTransactionID, MessageTransaction, MessageTransactionQuery> transactionStore;

  public PgQueueTransaction(Vertx vertx, JsonObject configuration) {
    this.transactionStore = new Repository<>(MessageTransactionMapper.INSTANCE, RepositoryHandler.leasePool(configuration, vertx));
  }


  @Override
  public <M> Uni<Void> transaction(Message<M> message, BiFunction<Message<M>, QueueTransaction, Uni<Void>> function) {
    return transactionStore.transaction(
      sqlConnection -> transactionStore.insert(
          new MessageTransaction(
            message.messageId(),
            null,
            message.payload().getClass().getName(),
            BaseRecord.newRecord(message.tenant())
          ),
          sqlConnection
        )
        .flatMap(avoid -> function.apply(message, new QueueTransaction(sqlConnection)))
    );
  }
}
