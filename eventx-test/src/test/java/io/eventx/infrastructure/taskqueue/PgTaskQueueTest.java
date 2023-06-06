package io.eventx.infrastructure.taskqueue;


import io.eventx.sql.Repository;
import io.eventx.sql.exceptions.NotFound;
import io.eventx.InfrastructureBootstrap;
import io.eventx.queue.models.QueueTransaction;
import io.eventx.queue.postgres.PgMessageProducer;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mutiny.core.Vertx;
import io.eventx.queue.models.Message;
import io.eventx.queue.postgres.mappers.DeadLetterMapper;
import io.eventx.queue.postgres.mappers.MessageQueueMapper;
import io.eventx.queue.postgres.mappers.MessageTransactionMapper;
import io.eventx.queue.postgres.models.DeadLetterKey;
import io.eventx.queue.postgres.models.MessageRecordID;
import io.eventx.queue.postgres.models.MessageTransactionID;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(VertxExtension.class)
public class PgTaskQueueTest {

  public static final InfrastructureBootstrap BOOTSTRAP = new InfrastructureBootstrap()
    .setPostgres(false)
    .addLiquibaseRun("task-queue.xml", Map.of("schema", "postgres"))
    .setConfigurationPath("fakeaggregate.json");

  @AfterAll
  static void destroy() throws Exception {
    BOOTSTRAP.destroy();
  }


  @Test
  void test_pg_producer(Vertx vertx, VertxTestContext vertxTestContext) {
    // todo produce to the db queue and assert record has been written
    final var producer = new PgMessageProducer(InfrastructureBootstrap.REPOSITORY_HANDLER);
    final var queue = new Repository<>(MessageQueueMapper.INSTANCE, InfrastructureBootstrap.REPOSITORY_HANDLER);
    final var fakeMessage = fakeMessage();
    InfrastructureBootstrap.REPOSITORY_HANDLER.pgPool().withTransaction(
      sqlConnection -> producer.enqueue(fakeMessage, new QueueTransaction(sqlConnection))
    ).await().indefinitely();
    queue.selectByKey(new MessageRecordID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
    vertxTestContext.completeNow();
  }

  @Test
  void test_pg_subscriber(Vertx vertx, VertxTestContext vertxTestContext) throws InterruptedException {
    final var producer = new PgMessageProducer(InfrastructureBootstrap.REPOSITORY_HANDLER);
    final var queueTx = new Repository<>(MessageTransactionMapper.INSTANCE, InfrastructureBootstrap.REPOSITORY_HANDLER);
    final var fakeMessage = fakeMessage();
    InfrastructureBootstrap.REPOSITORY_HANDLER.pgPool().withTransaction(
      sqlConnection -> producer.enqueue(fakeMessage, new QueueTransaction(sqlConnection))
    ).await().indefinitely();
    Thread.sleep(1000);
    queueTx.selectByKey(new MessageTransactionID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
    vertxTestContext.completeNow();
  }

  @Test
  void test_refresh_retry_and_dead_letter_queue(Vertx vertx, VertxTestContext vertxTestContext) throws InterruptedException {
    final var producer = new PgMessageProducer(InfrastructureBootstrap.REPOSITORY_HANDLER);
    final var deadLetters = new Repository<>(DeadLetterMapper.INSTANCE, InfrastructureBootstrap.REPOSITORY_HANDLER);
    final var fakeMessage = fakeDeadMessage();
    InfrastructureBootstrap.REPOSITORY_HANDLER.pgPool().withTransaction(
      sqlConnection -> producer.enqueue(fakeMessage, new QueueTransaction(sqlConnection))
    ).await().indefinitely();
    Thread.sleep(10000);
    deadLetters.selectByKey(new DeadLetterKey(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
    vertxTestContext.completeNow();
  }

  @Test
  void test_nack(Vertx vertx, VertxTestContext vertxTestContext) throws InterruptedException {
    final var producer = new PgMessageProducer(InfrastructureBootstrap.REPOSITORY_HANDLER);
    final var deadLetters = new Repository<>(DeadLetterMapper.INSTANCE, InfrastructureBootstrap.REPOSITORY_HANDLER);
    final var fakeMessage = fakeDeadMessage();
    InfrastructureBootstrap.REPOSITORY_HANDLER.pgPool().withTransaction(
      sqlConnection -> producer.enqueue(fakeMessage, new QueueTransaction(sqlConnection))
    ).await().indefinitely();
    Thread.sleep(1000);
    final var actualQueue = new Repository<>(MessageQueueMapper.INSTANCE, InfrastructureBootstrap.REPOSITORY_HANDLER);
    final var nackedMessage = actualQueue.selectByKey(new MessageRecordID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
    assertEquals(1, nackedMessage.retryCounter());
    assertThrows(NotFound.class, () -> deadLetters.selectByKey(new DeadLetterKey(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely());
    vertxTestContext.completeNow();
  }

  @Test
  void test_recovery_mechanism(Vertx vertx, VertxTestContext vertxTestContext) {
    // todo should only be processed if transaction is not present in the tx table.
    // todo should not process if present in tx table.
  }

  @NotNull
  private static Message<Object> fakeMessage() {
    return new Message<>(
      UUID.randomUUID().toString(),
      "default",
      null,
      null,
      0,
      new MockPayload("data")
    );
  }

  @NotNull
  private static Message<Object> fakeDeadMessage() {
    return new Message<>(
      UUID.randomUUID().toString(),
      "default",
      null,
      null,
      0,
      new MockDeadPayload("data")
    );
  }

}
