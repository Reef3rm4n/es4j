package io.es4j.infrastructure.pgbroker.vertx;


import io.es4j.infrastructure.pgbroker.messagebroker.PartitionHashRing;
import io.es4j.infrastructure.pgbroker.exceptions.ProducerExeception;
import io.es4j.infrastructure.pgbroker.mappers.MessageQueueMapper;
import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.sql.Repository;
import io.es4j.sql.RepositoryHandler;
import io.es4j.sql.models.BaseRecord;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.SqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class VertxMessageProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(VertxMessageProducer.class);

  private final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> queue;

  public VertxMessageProducer(RepositoryHandler repositoryHandler) {
    this.queue = new Repository<>(MessageQueueMapper.INSTANCE, repositoryHandler);
  }

  public <T> Uni<Void> enqueue(Message<T> message, ConsumerTransaction consumerTransaction) {
    log(message);
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
      null,
      PartitionHashRing.resolve(message.partitionKey()),
      message.partitionKey(),
      BaseRecord.newRecord(message.tenant())
    );
    return queue.insert(queueEntry, (SqlConnection) consumerTransaction.connection())
      .replaceWithVoid()
      .onFailure().transform(ProducerExeception::new);
  }

  private static <T> void log(Message<T> message) {
    LOGGER.debug("Enqueuing {}", message);
  }


  public <T> Uni<Void> enqueue(Message<T> message) {
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
      null,
      PartitionHashRing.resolve(message.partitionKey()),
      message.partitionKey(),
      BaseRecord.newRecord(message.tenant())
    );
    log(message);
    return queue.insert(queueEntry)
      .replaceWithVoid()
      .onFailure().transform(ProducerExeception::new);
  }

  public <T> Uni<Void> enqueue(List<Message<T>> entries, ConsumerTransaction consumerTransaction) {
    final var messageRecords = entries.stream().map(
      message -> {
        log(message);
        return new MessageRecord(
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
          null,
          PartitionHashRing.resolve(message.partitionKey()),
          message.partitionKey(),
          BaseRecord.newRecord(message.tenant())
        );
      }
    ).toList();
    return queue.insertBatch(messageRecords, consumerTransaction.getDelegate(SqlConnection.class))
      .replaceWithVoid()
      .onFailure().transform(ProducerExeception::new);
  }


  public <T> Uni<Void> enqueue(List<Message<T>> entries) {
    final var messageRecords = entries.stream().map(
      message -> {
        log(message);
        return new MessageRecord(
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
          null,
          PartitionHashRing.resolve(message.partitionKey()),
          message.partitionKey(),
          BaseRecord.newRecord(message.tenant())
        );
      }
    ).toList();
    return queue.insertBatch(messageRecords)
      .replaceWithVoid()
      .onFailure().transform(ProducerExeception::new);
  }

  public Uni<Void> cancel(MessageID messageID) {
    LOGGER.info("Cancelling message {}", messageID.id());
    return queue.deleteByKey(new MessageRecordID(messageID.id(), messageID.tenant()));
  }

  public <T> Uni<Message<T>> get(MessageID messageID, Class<T> tClass) {
    return queue.selectByKey(new MessageRecordID(messageID.id(), messageID.tenant()))
      .map(entry -> new Message<>(
          entry.id(),
        entry.baseRecord().tenant(),
        entry.partitionId(),
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
          entry.partitionId(),
          entry.scheduled(),
          entry.expiration(),
          entry.priority(),
          entry.payload().mapTo(tClass)
        )).toList()
      );
  }

}
