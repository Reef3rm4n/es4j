package io.es4j.infrastructure.pgbroker.vertx;


import io.es4j.infrastructure.pgbroker.core.PartitionHashRing;
import io.es4j.infrastructure.pgbroker.exceptions.ProducerExeception;
import io.es4j.infrastructure.pgbroker.mappers.MessageMapper;
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


public class PgClientTopicProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PgClientTopicProducer.class);
  private final Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> queue;

  public PgClientTopicProducer(RepositoryHandler repositoryHandler) {
    this.queue = new Repository<>(MessageMapper.INSTANCE, repositoryHandler);
  }

  public <T> Uni<Void> publish(TopicMessage<T> message, ConsumerTransaction consumerTransaction) {
    log(message);
    final var queueEntry = parse(message);
    return queue.insert(queueEntry, (SqlConnection) consumerTransaction.connection())
      .replaceWithVoid()
      .onFailure().transform(ProducerExeception::new);
  }

  public <T> Uni<Void> publish(TopicMessage<T> message) {
    final var queueEntry = parse(message);
    log(message);
    return queue.insert(queueEntry)
      .replaceWithVoid()
      .onFailure().transform(ProducerExeception::new);
  }

  public <T> Uni<Void> publish(List<TopicMessage<T>> entries, ConsumerTransaction consumerTransaction) {
    final var messageRecords = parse(entries);
    return queue.insertBatch(messageRecords, consumerTransaction.getDelegate(SqlConnection.class))
      .replaceWithVoid()
      .onFailure().transform(ProducerExeception::new);
  }

  public <T> Uni<Void> publish(List<TopicMessage<T>> entries) {
    final var messageRecords = parse(entries);
    return queue.insertBatch(messageRecords)
      .replaceWithVoid()
      .onFailure().transform(ProducerExeception::new);
  }

  public Uni<Void> cancel(MessageID messageID) {
    LOGGER.warn("Cancelling message -> {}", messageID.id());
    return queue.deleteByKey(new MessageRecordKey(messageID.id()));
  }

  private static <T> List<MessageRecord> parse(List<TopicMessage<T>> entries) {
    return entries.stream().map(PgClientTopicProducer::parse).toList();
  }

  private static <T> MessageRecord parse(TopicMessage<T> message) {
    return new MessageRecord(
      message.messageId(),
      null,
      null,
      null,
      MessageState.PUBLISHED,
      message.address(),
      (message.payload() instanceof JsonObject jsonObject) ? jsonObject : JsonObject.mapFrom(message.payload()),
      null,
      null,
      PartitionHashRing.resolve(message.partitionKey()),
      message.partitionKey(),
      message.schemaVersion(),
      BaseRecord.newRecord()
    );
  }


  private static <T> void log(TopicMessage<T> message) {
    LOGGER.debug("Publishing message -> {}", message);
  }

}
