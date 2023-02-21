package io.vertx.skeleton.ccp.producers;

import io.vertx.skeleton.ccp.models.*;
import io.vertx.skeleton.ccp.mappers.MessageQueueMapper;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.models.MessageState;
import io.vertx.skeleton.models.exceptions.OrmConnectionException;
import io.vertx.skeleton.sql.Repository;
import io.vertx.skeleton.sql.RepositoryHandler;
import io.vertx.skeleton.sql.models.RecordWithoutID;


import java.util.List;

public class PgQueueProducer {
  private final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> queue;
  private final FileSystemFallBack fallback;

  public PgQueueProducer(RepositoryHandler repositoryHandler) {
    this.queue = new Repository<>(MessageQueueMapper.INSTANCE, repositoryHandler);
    this.fallback = new FileSystemFallBack(queue);
  }

  public <T> Uni<Void> enqueue(Message<T> message) {
    final var queueEntry = new MessageRecord(
      message.id(),
      message.scheduled(),
      message.expiration(),
      message.priority(),
      0,
      MessageState.CREATED,
      message.payload().getClass().getName(),
      JsonObject.mapFrom(message.payload()),
      null,
      null,
      RecordWithoutID.newRecord(message.tenant())
    );
    return queue.insert(queueEntry).replaceWithVoid()
      .onFailure(OrmConnectionException.class).recoverWithUni(() -> fallback.load(queueEntry));
  }

  public <T> Uni<Void> enqueue(List<Message<T>> entries) {
    final var queueEntries = entries.stream().map(
      message -> new MessageRecord(
        message.id(),
        message.scheduled(),
        message.expiration(),
        message.priority(),
        0,
        MessageState.CREATED,
        message.payload().getClass().getName(),
        JsonObject.mapFrom(message.payload()),
        null,
        null,
        RecordWithoutID.newRecord(message.tenant())
      )
    ).toList();
    return queue.insertBatch(queueEntries).replaceWithVoid()
      .onFailure(OrmConnectionException.class).recoverWithUni(() -> fallback.load(queueEntries));
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

}
