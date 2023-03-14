package io.vertx.eventx.queue.postgres;

import io.activej.inject.Injector;
import io.smallrye.mutiny.Uni;
import io.vertx.eventx.queue.TransactionManager;
import io.vertx.eventx.queue.models.Message;
import io.vertx.eventx.queue.models.QueueTransaction;
import io.vertx.eventx.queue.postgres.mappers.MessageTransactionMapper;
import io.vertx.eventx.queue.postgres.models.MessageTransactionID;
import io.vertx.eventx.queue.postgres.models.MessageTransactionQuery;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.eventx.sql.models.BaseRecord;

import java.util.function.BiFunction;

public class PgTransaction implements TransactionManager {
  private final Repository<MessageTransactionID, io.vertx.eventx.queue.postgres.models.MessageTransaction, MessageTransactionQuery> transactionStore;

  public PgTransaction(Injector injector) {
    this.transactionStore = new Repository<>(MessageTransactionMapper.INSTANCE, injector.getInstance(RepositoryHandler.class));
  }


  @Override
  public <M> Uni<Void> transaction(Message<M> message, BiFunction<Message<M>, QueueTransaction, Uni<Void>> function) {
    return transactionStore.transaction(
      sqlConnection -> transactionStore.insert(
          new io.vertx.eventx.queue.postgres.models.MessageTransaction(
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
