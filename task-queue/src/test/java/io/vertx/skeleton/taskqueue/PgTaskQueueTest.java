package io.vertx.skeleton.taskqueue;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mutiny.core.Vertx;
import io.vertx.skeleton.sql.Repository;
import io.vertx.skeleton.sql.exceptions.OrmNotFoundException;
import io.vertx.skeleton.taskqueue.models.Message;
import io.vertx.skeleton.taskqueue.models.TaskTransaction;
import io.vertx.skeleton.taskqueue.postgres.PgTaskProducer;
import io.vertx.skeleton.taskqueue.postgres.mappers.DeadLetterMapper;
import io.vertx.skeleton.taskqueue.postgres.mappers.MessageQueueMapper;
import io.vertx.skeleton.taskqueue.postgres.mappers.MessageTransactionMapper;
import io.vertx.skeleton.taskqueue.postgres.models.DeadLetterKey;
import io.vertx.skeleton.taskqueue.postgres.models.MessageRecordID;
import io.vertx.skeleton.taskqueue.postgres.models.MessageTransactionID;
import io.vertx.skeleton.test.VertxTestBootstrap;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(VertxExtension.class)
public class PgTaskQueueTest {

  public static final VertxTestBootstrap BOOTSTRAP = new VertxTestBootstrap()
    .setPostgres(true)
    .addLiquibaseRun("task-queue.xml", Map.of("schema", "postgres"))
    .setConfigurationPath("config.json");

  @BeforeAll
  static void prepare() throws Exception {
    BOOTSTRAP.bootstrap();
  }

  @AfterAll
  static void destroy() throws Exception {
    BOOTSTRAP.destroy();
  }


  @Test
  void test_pg_producer(Vertx vertx, VertxTestContext vertxTestContext) {
    // todo produce to the db queue and assert record has been written
    final var producer = new PgTaskProducer(BOOTSTRAP.REPOSITORY_HANDLER);
    final var queue = new Repository<>(MessageQueueMapper.INSTANCE, BOOTSTRAP.REPOSITORY_HANDLER);
    final var fakeMessage = fakeMessage();
    BOOTSTRAP.REPOSITORY_HANDLER.pgPool().withTransaction(
      sqlConnection -> producer.enqueue(fakeMessage, new TaskTransaction(sqlConnection))
    ).await().indefinitely();
    queue.selectByKey(new MessageRecordID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
    vertxTestContext.completeNow();
  }

  @Test
  void test_pg_subscriber(Vertx vertx, VertxTestContext vertxTestContext) throws InterruptedException {
    final var producer = new PgTaskProducer(BOOTSTRAP.REPOSITORY_HANDLER);
    final var queueTx = new Repository<>(MessageTransactionMapper.INSTANCE, BOOTSTRAP.REPOSITORY_HANDLER);
    final var fakeMessage = fakeMessage();
    BOOTSTRAP.REPOSITORY_HANDLER.pgPool().withTransaction(
      sqlConnection -> producer.enqueue(fakeMessage, new TaskTransaction(sqlConnection))
    ).await().indefinitely();
    Thread.sleep(1000);
    queueTx.selectByKey(new MessageTransactionID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
    vertxTestContext.completeNow();
  }

  @Test
  void test_refresh_retry_and_dead_letter_queue(Vertx vertx, VertxTestContext vertxTestContext) throws InterruptedException {
    final var producer = new PgTaskProducer(BOOTSTRAP.REPOSITORY_HANDLER);
    final var deadLetters = new Repository<>(DeadLetterMapper.INSTANCE, BOOTSTRAP.REPOSITORY_HANDLER);
    final var fakeMessage = fakeDeadMessage();
    BOOTSTRAP.REPOSITORY_HANDLER.pgPool().withTransaction(
      sqlConnection -> producer.enqueue(fakeMessage, new TaskTransaction(sqlConnection))
    ).await().indefinitely();
    Thread.sleep(10000);
    deadLetters.selectByKey(new DeadLetterKey(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
    vertxTestContext.completeNow();
  }

  @Test
  void test_nack(Vertx vertx, VertxTestContext vertxTestContext) throws InterruptedException {
    final var producer = new PgTaskProducer(BOOTSTRAP.REPOSITORY_HANDLER);
    final var deadLetters = new Repository<>(DeadLetterMapper.INSTANCE, BOOTSTRAP.REPOSITORY_HANDLER);
    final var fakeMessage = fakeDeadMessage();
    BOOTSTRAP.REPOSITORY_HANDLER.pgPool().withTransaction(
      sqlConnection -> producer.enqueue(fakeMessage, new TaskTransaction(sqlConnection))
    ).await().indefinitely();
    Thread.sleep(1000);
    final var actualQueue = new Repository<>(MessageQueueMapper.INSTANCE, BOOTSTRAP.REPOSITORY_HANDLER);
    final var nackedMessage = actualQueue.selectByKey(new MessageRecordID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
    assertEquals(1, nackedMessage.retryCounter());
    assertThrows(OrmNotFoundException.class, () -> deadLetters.selectByKey(new DeadLetterKey(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely());
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
