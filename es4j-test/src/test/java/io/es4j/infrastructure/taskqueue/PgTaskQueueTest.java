//package io.es4j.infrastructure.taskqueue;
//
//
//import io.es4j.core.verticles.TaskProcessorVerticle;
//import io.es4j.sql.Repository;
//import io.es4j.sql.exceptions.NotFound;
//import io.es4j.InfrastructureBootstrap;
//import io.es4j.infrastructure.messagebroker.models.QueueTransaction;
//import io.es4j.infrastructure.messagebroker.postgres.PgMessageProducer;
//import io.es4j.infrastructure.messagebroker.models.Message;
//import io.es4j.infrastructure.messagebroker.postgres.mappers.DeadLetterMapper;
//import io.es4j.infrastructure.messagebroker.postgres.mappers.MessageQueueMapper;
//import io.es4j.infrastructure.messagebroker.postgres.mappers.MessageTransactionMapper;
//import io.es4j.infrastructure.messagebroker.postgres.models.DeadLetterKey;
//import io.es4j.infrastructure.messagebroker.postgres.models.MessageRecordID;
//import io.es4j.infrastructure.messagebroker.postgres.models.MessageTransactionID;
//import org.jetbrains.annotations.NotNull;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Test;
//import java.util.Map;
//import java.util.UUID;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//public class PgTaskQueueTest {
//
//  public static final InfrastructureBootstrap BOOTSTRAP = new InfrastructureBootstrap()
//    .setPostgres(true)
//    .addLiquibaseRun("task-queue.xml", Map.of("schema", "es4j"));
//
//  @AfterAll
//  static void destroy() throws Exception {
//    BOOTSTRAP.destroy();
//  }
//
//  @BeforeAll
//  static void start() {
//    BOOTSTRAP.start();
//    TaskProcessorVerticle.deploy(InfrastructureBootstrap.vertx, BOOTSTRAP.CONFIGURATION).await().indefinitely();
//  }
//
//
//  @Test
//  void test_pg_producer() {
//    // todo produce to the db queue and assert record has been written
//    final var producer = new PgMessageProducer(InfrastructureBootstrap.REPOSITORY_HANDLER);
//    final var queue = new Repository<>(MessageQueueMapper.INSTANCE, InfrastructureBootstrap.REPOSITORY_HANDLER);
//    final var fakeMessage = fakeMessage();
//    InfrastructureBootstrap.REPOSITORY_HANDLER.pgPool().withTransaction(
//      sqlConnection -> producer.enqueue(fakeMessage, new QueueTransaction(sqlConnection))
//    ).await().indefinitely();
//    queue.selectByKey(new MessageRecordID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
//  }
//
//  @Test
//  void test_pg_subscriber() throws InterruptedException {
//    final var producer = new PgMessageProducer(InfrastructureBootstrap.REPOSITORY_HANDLER);
//    final var queueTx = new Repository<>(MessageTransactionMapper.INSTANCE, InfrastructureBootstrap.REPOSITORY_HANDLER);
//    final var fakeMessage = fakeMessage();
//    InfrastructureBootstrap.REPOSITORY_HANDLER.pgPool().withTransaction(
//      sqlConnection -> producer.enqueue(fakeMessage, new QueueTransaction(sqlConnection))
//    ).await().indefinitely();
//    Thread.sleep(1000);
//    queueTx.selectByKey(new MessageTransactionID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
//  }
//
//  @Test
//  void test_refresh_retry_and_dead_letter_queue() throws InterruptedException {
//    final var producer = new PgMessageProducer(InfrastructureBootstrap.REPOSITORY_HANDLER);
//    final var deadLetters = new Repository<>(DeadLetterMapper.INSTANCE, InfrastructureBootstrap.REPOSITORY_HANDLER);
//    final var fakeMessage = fakeFatal();
//    InfrastructureBootstrap.REPOSITORY_HANDLER.pgPool().withTransaction(
//      sqlConnection -> producer.enqueue(fakeMessage, new QueueTransaction(sqlConnection))
//    ).await().indefinitely();
//    Thread.sleep(1000);
//    deadLetters.selectByKey(new DeadLetterKey(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
//  }
//
//  @Test
//  void test_nack() throws InterruptedException {
//    final var producer = new PgMessageProducer(InfrastructureBootstrap.REPOSITORY_HANDLER);
//    final var deadLetters = new Repository<>(DeadLetterMapper.INSTANCE, InfrastructureBootstrap.REPOSITORY_HANDLER);
//    final var fakeMessage = fakeDeadMessage();
//    InfrastructureBootstrap.REPOSITORY_HANDLER.pgPool().withTransaction(
//      sqlConnection -> producer.enqueue(fakeMessage, new QueueTransaction(sqlConnection))
//    ).await().indefinitely();
//    Thread.sleep(1000);
//    final var actualQueue = new Repository<>(MessageQueueMapper.INSTANCE, InfrastructureBootstrap.REPOSITORY_HANDLER);
//    final var nackedMessage = actualQueue.selectByKey(new MessageRecordID(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely();
//    assertNotEquals(0, nackedMessage.retryCounter());
//    assertThrows(NotFound.class, () -> deadLetters.selectByKey(new DeadLetterKey(fakeMessage.messageId(), fakeMessage.tenant())).await().indefinitely());
//  }
//
//  @Test
//  void test_recovery_mechanism() {
//    // todo should only be processed if transaction is not present in the tx table.
//    // todo should not process if present in tx table.
//  }
//
//  @NotNull
//  private static Message<Object> fakeMessage() {
//    return new Message<>(
//      UUID.randomUUID().toString(),
//      "default",
//      null,
//      null,
//      0,
//      new MockPayload("data")
//    );
//  }
//
//  @NotNull
//  private static Message<Object> fakeFatal() {
//    return new Message<>(
//      UUID.randomUUID().toString(),
//      "default",
//      null,
//      null,
//      0,
//      new MockDeadPayload("data", true)
//    );
//  }
//
//  @NotNull
//  private static Message<Object> fakeDeadMessage() {
//    return new Message<>(
//      UUID.randomUUID().toString(),
//      "default",
//      null,
//      null,
//      0,
//      new MockDeadPayload("data", false)
//    );
//  }
//}
