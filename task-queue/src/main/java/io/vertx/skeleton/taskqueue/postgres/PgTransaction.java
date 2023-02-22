package io.vertx.skeleton.taskqueue.postgres;

import io.activej.inject.Injector;
import io.smallrye.mutiny.Uni;
import io.vertx.skeleton.taskqueue.TransactionManager;
import io.vertx.skeleton.taskqueue.models.Message;
import io.vertx.skeleton.taskqueue.models.TaskTransaction;
import io.vertx.skeleton.taskqueue.postgres.mappers.MessageTransactionMapper;
import io.vertx.skeleton.taskqueue.postgres.models.MessageTransaction;
import io.vertx.skeleton.taskqueue.postgres.models.MessageTransactionID;
import io.vertx.skeleton.taskqueue.postgres.models.MessageTransactionQuery;
import io.vertx.skeleton.sql.Repository;
import io.vertx.skeleton.sql.RepositoryHandler;
import io.vertx.skeleton.sql.models.RecordWithoutID;

import java.util.function.BiFunction;

public class PgTransaction implements TransactionManager {
  private final Repository<MessageTransactionID, MessageTransaction, MessageTransactionQuery> transactionStore;

  public PgTransaction(Injector injector) {
    this.transactionStore = new Repository<>(MessageTransactionMapper.INSTANCE, injector.getInstance(RepositoryHandler.class));
  }


  @Override
  public <M> Uni<Void> transaction(Message<M> message, BiFunction<Message<M>, TaskTransaction, Uni<Void>> function) {
    return transactionStore.transaction(
      sqlConnection -> transactionStore.insert(
          new MessageTransaction(
            message.messageId(),
            null,
            message.payload().getClass().getName(),
            RecordWithoutID.newRecord(message.tenant())
          ),
          sqlConnection
        )
        .flatMap(avoid -> function.apply(message, new TaskTransaction(sqlConnection)))
    );
  }
}
