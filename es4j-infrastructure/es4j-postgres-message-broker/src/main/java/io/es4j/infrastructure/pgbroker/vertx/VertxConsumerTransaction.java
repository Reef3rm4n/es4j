package io.es4j.infrastructure.pgbroker.vertx;

import io.es4j.infrastructure.pgbroker.ConsumerTransactionProvider;
import io.es4j.infrastructure.pgbroker.exceptions.DuplicateMessage;
import io.es4j.infrastructure.pgbroker.mappers.MessageTransactionMapper;
import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.sql.Repository;
import io.es4j.sql.RepositoryHandler;
import io.es4j.sql.exceptions.Conflict;
import io.es4j.sql.models.BaseRecord;
import io.smallrye.mutiny.Uni;

import java.util.function.BiFunction;

public class VertxConsumerTransaction implements ConsumerTransactionProvider {
  private Repository<ConsumerTransactionKey, ConsumerTransactionRecord, ConsumerTransactionQuery> transactionStore;

  @Override
  public void start(RepositoryHandler repositoryHandler) {
    this.transactionStore = new Repository<>(MessageTransactionMapper.INSTANCE, repositoryHandler);
  }


  @Override
  public <T> Uni<T> transaction(String consumer, RawMessage message, BiFunction<RawMessage, ConsumerTransaction, Uni<T>> function) {
    return transactionStore.transaction(sqlConnection -> transactionStore.insert(
          new ConsumerTransactionRecord(
            message.messageId(),
            consumer,
            BaseRecord.newRecord(message.tenant())
          ),
          sqlConnection
        )
        .onFailure(Conflict.class).transform(DuplicateMessage::new)
        .flatMap(avoid -> function.apply(message, new ConsumerTransaction(sqlConnection)))
    );
  }
}
