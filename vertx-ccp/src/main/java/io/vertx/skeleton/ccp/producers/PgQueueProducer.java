package io.vertx.skeleton.ccp.producers;

import io.vertx.skeleton.ccp.models.*;
import io.vertx.skeleton.orm.Repository;
import io.vertx.skeleton.orm.RepositoryHandler;
import io.vertx.skeleton.ccp.mappers.MessageQueueMapper;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.models.MessageState;
import io.vertx.skeleton.models.exceptions.OrmConnectionException;
import io.vertx.skeleton.models.PersistedRecord;


import java.util.List;

public class PgQueueProducer<T> {
  private final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> queue;
  private final FileSystemFallBack<T> fallback;

  public PgQueueProducer(RepositoryHandler repositoryHandler, String queueName) {
    this.queue = new Repository<>(new MessageQueueMapper(queueName), repositoryHandler);
    this.fallback = new FileSystemFallBack<T>(queue);
  }

  public Uni<Void> enqueue(Message<T> message) {
    final var queueEntry = new MessageRecord(
      message.id(),
      message.scheduled(),
      message.expiration(),
      message.priority(),
      0,
      MessageState.CREATED,
      JsonObject.mapFrom(message.payload()),
      null,
      null,
      PersistedRecord.newRecord(message.tenant())
    );
    return queue.insert(queueEntry).replaceWithVoid()
      .onFailure(OrmConnectionException.class).recoverWithUni(() -> fallback.load(queueEntry));
  }

  public Uni<Void> enqueue(List<Message<T>> entries) {
    final var queueEntries = entries.stream().map(
      message -> new MessageRecord(
        message.id(),
        message.scheduled(),
        message.expiration(),
        message.priority(),
        0,
        MessageState.CREATED,
        JsonObject.mapFrom(message.payload()),
        null,
        null,
        PersistedRecord.newRecord(message.tenant())
      )
    ).toList();
    return queue.insertBatch(queueEntries).replaceWithVoid()
      .onFailure(OrmConnectionException.class).recoverWithUni(() -> fallback.load(queueEntries));
  }

  public Uni<Void> cancel(MessageID messageID) {
    return queue.deleteByKey(new MessageRecordID(messageID.id(), messageID.tenant()));
  }

  public Uni<Message<T>> get(MessageID messageID, Class<T> tClass) {
    return queue.selectByKey(new MessageRecordID(messageID.id(), messageID.tenant()))
      .map(entry -> new Message<>(
          entry.id(),
          entry.persistedRecord().tenant(),
          entry.scheduled(),
          entry.expiration(),
          entry.priority(),
          entry.payload().mapTo(tClass)
        )
      );
  }

  public Uni<List<Message<T>>> query(MessageRecordQuery query, Class<T> tClass) {
    return queue.query(query)
      .map(entries -> entries.stream()
        .map(entry -> new Message<>(
          entry.id(),
          entry.persistedRecord().tenant(),
          entry.scheduled(),
          entry.expiration(),
          entry.priority(),
          entry.payload().mapTo(tClass)
        )).toList()
      );
  }

}
