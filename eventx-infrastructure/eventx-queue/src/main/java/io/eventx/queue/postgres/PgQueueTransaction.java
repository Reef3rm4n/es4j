package io.eventx.queue.postgres;

import io.activej.inject.Injector;
import io.eventx.queue.postgres.mappers.MessageTransactionMapper;
import io.eventx.queue.postgres.models.MessageTransaction;
import io.eventx.queue.postgres.models.MessageTransactionID;
import io.eventx.queue.postgres.models.MessageTransactionQuery;
import io.eventx.sql.Repository;
import io.eventx.sql.RepositoryHandler;
import io.eventx.sql.models.BaseRecord;
import io.smallrye.mutiny.Uni;
import io.eventx.queue.QueueTransactionManager;
import io.eventx.queue.models.Message;
import io.eventx.queue.models.QueueTransaction;

import java.util.function.BiFunction;

public class PgQueueTransaction implements QueueTransactionManager {
  private final Repository<MessageTransactionID, MessageTransaction, MessageTransactionQuery> transactionStore;

  public PgQueueTransaction(Injector injector) {
    this.transactionStore = new Repository<>(MessageTransactionMapper.INSTANCE, injector.getInstance(RepositoryHandler.class));
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