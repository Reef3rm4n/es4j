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
  private Repository<MessageTransactionID, MessageTransaction, MessageTransactionQuery> transactionStore;

  @Override
  public void start(RepositoryHandler repositoryHandler) {
    this.transactionStore = new Repository<>(MessageTransactionMapper.INSTANCE, repositoryHandler);
  }

  @Override
  public <M> Uni<Void> transaction(String processorClass, Message<M> message, BiFunction<Message<M>, ConsumerTransaction, Uni<Void>> function) {
    return transactionStore.transaction(sqlConnection -> transactionStore.insert(
          new MessageTransaction(
            message.messageId(),
            processorClass,
            message.payload().getClass().getName(),
            BaseRecord.newRecord(message.tenant())
          ),
          sqlConnection
        )
        .onFailure(Conflict.class).transform(DuplicateMessage::new)
        .flatMap(avoid -> function.apply(message, new ConsumerTransaction(sqlConnection)))
    );
  }
}
