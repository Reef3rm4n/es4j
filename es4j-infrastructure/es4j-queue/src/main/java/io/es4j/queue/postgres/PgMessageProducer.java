package io.es4j.queue.postgres;

import io.es4j.queue.models.Message;
import io.es4j.queue.models.MessageID;
import io.es4j.queue.models.MessageState;
import io.es4j.queue.models.QueueTransaction;
import io.es4j.queue.postgres.mappers.MessageQueueMapper;
import io.es4j.queue.postgres.models.MessageRecord;
import io.es4j.queue.postgres.models.MessageRecordID;
import io.es4j.queue.postgres.models.MessageRecordQuery;
import io.es4j.sql.Repository;
import io.es4j.sql.RepositoryHandler;
import io.es4j.sql.models.BaseRecord;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.es4j.queue.MessageProducer;


import java.util.List;
import java.util.function.Consumer;

public class PgMessageProducer implements MessageProducer {
  private final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> queue;

  public PgMessageProducer(RepositoryHandler repositoryHandler) {
    this.queue = new Repository<>(MessageQueueMapper.INSTANCE, repositoryHandler);
  }

  public <T> Uni<Void> enqueue(Message<T> message, QueueTransaction queueTransaction) {
    final var queueEntry = new MessageRecord(
      message.messageId(),
      message.scheduled(),
      message.expiration(),
      message.priority(),
      0,
      MessageState.CREATED,
      message.payload().getClass().getName(),
      JsonObject.mapFrom(message.payload()),
      null,
      null,
      BaseRecord.newRecord(message.tenant())
    );
    return queue.insert(queueEntry, (SqlConnection) queueTransaction.connection()).replaceWithVoid();
  }

  public <T> Uni<Void> enqueue(List<Message<T>> entries) {
    final var queueEntries = entries.stream().map(
      message -> new MessageRecord(
        message.messageId(),
        message.scheduled(),
        message.expiration(),
        message.priority(),
        0,
        MessageState.CREATED,
        message.payload().getClass().getName(),
        JsonObject.mapFrom(message.payload()),
        null,
        null,
        BaseRecord.newRecord(message.tenant())
      )
    ).toList();
    return queue.insertBatch(queueEntries).replaceWithVoid();
  }

  public <T> Uni<Void> enqueue(List<Message<T>> entries, QueueTransaction queueTransaction) {
    final var queueEntries = entries.stream().map(
      message -> new MessageRecord(
        message.messageId(),
        message.scheduled(),
        message.expiration(),
        message.priority(),
        0,
        MessageState.CREATED,
        message.payload().getClass().getName(),
        JsonObject.mapFrom(message.payload()),
        null,
        null,
        BaseRecord.newRecord(message.tenant())
      )
    ).toList();
    return queue.insertBatch(queueEntries, (SqlConnection) queueTransaction.connection()).replaceWithVoid();
  }

  public Uni<Void> cancel(MessageID messageID) {
    return queue.deleteByKey(new MessageRecordID(messageID.id(), messageID.tenant()));
  }

  public <T> Uni<Message<T>> get(MessageID messageID, Class<T> tClass) {
    return queue.selectByKey(new MessageRecordID(messageID.id(), messageID.tenant()))
      .map(entry -> new Message<>(
          entry.id(),
          entry.baseRecord().tenant(),
          entry.scheduled(),
          entry.expiration(),
          entry.priority(),
          entry.payload().mapTo(tClass)
        )
      );
  }

  public <T> Uni<List<Message<T>>> query(MessageRecordQuery query, Class<T> tClass) {
    return queue.query(query)
      .map(entries -> entries.stream()
        .map(entry -> new Message<>(
          entry.id(),
          entry.baseRecord().tenant(),
          entry.scheduled(),
          entry.expiration(),
          entry.priority(),
          entry.payload().mapTo(tClass)
        )).toList()
      );
  }
  public <T> Uni<Void> stream(MessageRecordQuery query, Class<T> tClass, Consumer<Message<T>> messageConsumer) {
    return queue.stream(entry -> {
        final var msg = new Message<>(
          entry.id(),
          entry.baseRecord().tenant(),
          entry.scheduled(),
          entry.expiration(),
          entry.priority(),
          entry.payload().mapTo(tClass)
        );
        messageConsumer.accept(msg);
      },
      query
    );
  }

}
