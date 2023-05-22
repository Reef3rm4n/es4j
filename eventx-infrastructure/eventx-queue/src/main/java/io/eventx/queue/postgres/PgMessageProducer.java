package io.eventx.queue.postgres;

import io.eventx.queue.models.Message;
import io.eventx.queue.models.MessageID;
import io.eventx.queue.models.MessageState;
import io.eventx.queue.models.QueueTransaction;
import io.eventx.queue.postgres.mappers.MessageQueueMapper;
import io.eventx.queue.postgres.models.MessageRecord;
import io.eventx.queue.postgres.models.MessageRecordID;
import io.eventx.queue.postgres.models.MessageRecordQuery;
import io.eventx.sql.Repository;
import io.eventx.sql.RepositoryHandler;
import io.eventx.sql.models.BaseRecord;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.eventx.queue.MessageProducer;


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
          entry.baseRecord().tenantId(),
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
          entry.baseRecord().tenantId(),
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
          entry.baseRecord().tenantId(),
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
