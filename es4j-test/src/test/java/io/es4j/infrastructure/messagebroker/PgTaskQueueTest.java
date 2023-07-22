package io.es4j.infrastructure.messagebroker;

import io.es4j.infrastructure.pgbroker.PgBroker;
import io.es4j.infrastructure.pgbroker.mappers.DeadLetterMapper;
import io.es4j.infrastructure.pgbroker.mappers.MessageQueueMapper;
import io.es4j.infrastructure.pgbroker.mappers.MessageTransactionMapper;
import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.infrastructure.pgbroker.vertx.VertxMessageProducer;
import io.es4j.infrastructure.sql.SqlBootstrap;
import io.es4j.sql.Repository;
import io.es4j.sql.exceptions.NotFound;
import io.es4j.sql.models.QueryOptions;
import io.smallrye.mutiny.Uni;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class PgTaskQueueTest {

  public static final SqlBootstrap BOOTSTRAP = new SqlBootstrap().setPostgres(false);
  final VertxMessageProducer producer = new VertxMessageProducer(SqlBootstrap.REPOSITORY_HANDLER);
  final Repository<MessageTransactionID, MessageTransaction, MessageTransactionQuery> queueTx = new Repository<>(MessageTransactionMapper.INSTANCE, SqlBootstrap.REPOSITORY_HANDLER);
  final Repository<DeadLetterKey, DeadLetterRecord, MessageRecordQuery> deadLetters = new Repository<>(DeadLetterMapper.INSTANCE, SqlBootstrap.REPOSITORY_HANDLER);
  final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue = new Repository<>(MessageQueueMapper.INSTANCE, SqlBootstrap.REPOSITORY_HANDLER);

  @AfterAll
  static void destroy() throws Exception {
    BOOTSTRAP.destroy();
  }

  @BeforeAll
  static void start() {
    BOOTSTRAP.start();
    PgBroker.deploy(BOOTSTRAP.CONFIGURATION, SqlBootstrap.vertx).await().indefinitely();
  }


  @Test
  void test_pg_producer() {
    final var fakeMessage = fakeMessage();
    sendMessage(fakeMessage);
    messageQueue.selectByKey(new MessageRecordID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
  }

  @Test
  void test_unpartitioned_message() throws InterruptedException {
    final var fakeMessage = fakeMessage();
    sendMessage(fakeMessage);
    Thread.sleep(5000);
    queueTx.selectByKey(new MessageTransactionID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
  }

  @Test
  void test_partitioned_message() throws InterruptedException {
    final var fakeMessage = fakePartitonedMessage();
    sendMessage(fakeMessage);
    Thread.sleep(1000);
    queueTx.selectByKey(new MessageTransactionID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
  }

  @Test
  void test_partition_distribution() throws InterruptedException {
    final var messages = IntStream.range(0, 10).mapToObj(i -> fakePartitonedMessage()).toList();
    sendMessages(messages);
    Thread.sleep(1000);
    final var txs = queueTx.query(
      new MessageTransactionQuery(
        messages.stream().map(Message::messageId).toList(),
        null,
        QueryOptions.simple()
      )
    ).await().indefinitely();
    Assertions.assertEquals(messages.size(), txs.size());
  }

  private void sendMessages(List<Message<Object>> messages) {
    producer.enqueue(messages).await().indefinitely();
  }

  @Test
  void test_dead_letter_fatal_message() throws InterruptedException {
    final var fakeMessage = fatalMessage();
    sendMessage(fakeMessage);
    Thread.sleep(2000);
    deadLetters.selectByKey(new DeadLetterKey(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
  }

  @Test
  void test_dead_letter_fatal_message_partition() throws InterruptedException {
    final var partionKey = "dummy-key";
    final var fakeMessage = fatalMessage(partionKey);
    sendMessage(fakeMessage);
    Thread.sleep(2000);
    deadLetters.selectByKey(new DeadLetterKey(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
  }

  @Test
  void test_parking_after_dead_letter_partition() throws InterruptedException {
    final var partionKey = "dummy-key";
    final var fakeMessage = fatalMessage(partionKey);
    sendMessage(fakeMessage);
    Thread.sleep(2000);
    deadLetters.selectByKey(new DeadLetterKey(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
    final var toBeParked = IntStream.range(0, 10).mapToObj(i -> fatalMessage(partionKey)).toList();
    sendMessages(toBeParked);
    Thread.sleep(2000);
    final var parkedMessages = deadLetters.query(
      MessageRecordQueryBuilder.builder()
        .ids(toBeParked.stream().map(Message::messageId).toList())
        .options(QueryOptions.simple())
        .build()
    ).await().indefinitely();
    Assertions.assertEquals(toBeParked.size(), parkedMessages.size());
    Assertions.assertTrue(parkedMessages.stream().allMatch(m -> m.messageState() == MessageState.PARKED));
  }

  @Test
  void test_nack() throws InterruptedException {
    final var fakeMessage = failForRetryMessage();
    sendMessage(fakeMessage);
    Thread.sleep(1000);
    final var actualQueue = new Repository<>(MessageQueueMapper.INSTANCE, SqlBootstrap.REPOSITORY_HANDLER);
    final var nackedMessage = actualQueue.selectByKey(new MessageRecordID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
    assertNotEquals(0, nackedMessage.retryCounter());
    assertThrows(NotFound.class, () -> deadLetters.selectByKey(new DeadLetterKey(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely());
  }

  @Test
  void test_recovery_mechanism() {


  }

  private void sendMessage(Message<Object> fakeMessage) {
    SqlBootstrap.REPOSITORY_HANDLER.pgPool().withTransaction(
      sqlConnection -> producer.enqueue(fakeMessage, new ConsumerTransaction(sqlConnection))
    ).await().indefinitely();
  }

  private Uni<Void> sendMessageUni(Message<Object> fakeMessage) {
    return SqlBootstrap.REPOSITORY_HANDLER.pgPool().withTransaction(
      sqlConnection -> producer.enqueue(fakeMessage, new ConsumerTransaction(sqlConnection))
    );
  }

  @NotNull
  private static Message<Object> fakeMessage() {
    return new Message<>(
      UUID.randomUUID().toString(),
      "default",
      null,
      null,
      null,
      0,
      new MockPayload("data")
    );
  }

  @NotNull
  private static Message<Object> fakePartitonedMessage() {
    final var id = UUID.randomUUID().toString();
    return new Message<>(
      id,
      "default",
      id,
      null,
      null,
      0,
      new MockPayload("data")
    );
  }

  @NotNull
  private static Message<Object> fatalMessage() {
    return new Message<>(
      UUID.randomUUID().toString(),
      "default",
      null,
      null,
      null,
      0,
      new MockDeadPayload("data", true)
    );
  }
  @NotNull
  private static Message<Object> fatalMessage(String partitionKey) {
    return new Message<>(
      UUID.randomUUID().toString(),
      "default",
      partitionKey,
      null,
      null,
      0,
      new MockDeadPayload("data", true)
    );
  }

  @NotNull
  private static Message<Object> failForRetryMessage() {
    return new Message<>(
      UUID.randomUUID().toString(),
      "default",
      null,
      null,
      null,
      0,
      new MockDeadPayload("data", false)
    );
  }
}
