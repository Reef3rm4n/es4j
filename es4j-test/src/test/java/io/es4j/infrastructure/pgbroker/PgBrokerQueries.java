package io.es4j.infrastructure.pgbroker;

import io.es4j.infrastructure.pgbroker.core.SessionRefresher;
import io.es4j.infrastructure.pgbroker.core.TopicPartitionPollingSession;
import io.es4j.infrastructure.pgbroker.mappers.MessageMapper;
import io.es4j.infrastructure.pgbroker.mappers.MessageTransactionMapper;
import io.es4j.infrastructure.pgbroker.mappers.BrokerPartitionMapper;
import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.infrastructure.sql.SqlBootstrap;
import io.es4j.sql.Repository;
import io.es4j.sql.exceptions.NotFound;
import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.BaseRecordBuilder;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class PgBrokerQueries {
  public static final SqlBootstrap BOOTSTRAP = new SqlBootstrap().setPostgres(true);
  final Repository<ConsumerTransactionKey, ConsumerTransactionRecord, ConsumerTransactionQuery> consumerTransaction = new Repository<>(MessageTransactionMapper.INSTANCE, SqlBootstrap.REPOSITORY_HANDLER);
  final Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageBroker = new Repository<>(MessageMapper.INSTANCE, SqlBootstrap.REPOSITORY_HANDLER);
  private Repository<PartitionKey, BrokerPartitionRecord, PartitionQuery> brokerPartitions = new Repository<>(BrokerPartitionMapper.INSTANCE, SqlBootstrap.REPOSITORY_HANDLER);


  @AfterAll
  static void destroy() {
    BOOTSTRAP.destroy();
  }

  @BeforeAll
  static void start() {
    BOOTSTRAP.start();
    PgBroker.deploy(BOOTSTRAP.CONFIGURATION, SqlBootstrap.vertx).await().indefinitely();
    PgBroker.undeploy(SqlBootstrap.vertx).await().indefinitely();
  }

  @Test
  void test_message_purge_statement_empty_queue() {
    assertThrowsExactly(
      NotFound.class,
      () -> messageBroker.query(SessionRefresher.MESSAGE_PURGE_STATEMENT.formatted(Duration.ofDays(1).toDays())).await().indefinitely()
    );
  }

  @Test
  void test_message_purge_statement() {

  }

  @Test
  void test_tx_purge_statement_empty() {
    assertThrowsExactly(
      NotFound.class,
      () -> consumerTransaction.query(SessionRefresher.TX_PURGE_STATEMENT.formatted(Duration.ofDays(1).toDays())).await().indefinitely()
    );
  }

  @Test
  void test_tx_purge_statement() {

  }

  @Test
  void test_stuck_messages_empty() {
    assertThrowsExactly(
      NotFound.class,
      () -> messageBroker.query(SessionRefresher.STUCK_MESSAGES_STATEMENT.formatted(Duration.ofDays(1).toDays())).await().indefinitely()
    );
  }

  @Test
  void test_stuck_messages() {
    final var message = MessageRecordBuilder.builder(topicMessage())
      .messageState(MessageState.CONSUMING)
      .build();
    assertDoesNotThrow(() -> messageBroker.insert(message).await().indefinitely());
    assertDoesNotThrow(
      () -> messageBroker.repositoryHandler().sqlClient().query("update message_broker set updated = updated - interval '1 hour' where message_id = '" + message.messageId()+"' returning *;"
      ).execute().await().indefinitely()
    );
    assertDoesNotThrow(
      () -> messageBroker.repositoryHandler().sqlClient().query(SessionRefresher.STUCK_MESSAGES_STATEMENT.formatted(Duration.ofMinutes(30).getSeconds())).execute().await().indefinitely()
    );
   final var result = Assertions.assertDoesNotThrow(
           () -> messageBroker.selectByKey(new MessageRecordKey(message.messageId())).await().indefinitely()
   );
   assertEquals(MessageState.STUCK, result.messageState());
  }


  @Test
  void test_queue_claim_statement() {

  }

  @Test
  void test_topic_claim_statement() {

  }

  @Test
  void test_topic_partition_claim() {
    final var partition = BrokerPartitionRecordBuilder.builder()
      .partitionId(UUID.randomUUID().toString())
      .baseRecord(BaseRecord.newRecord())
      .locked(false)
      .deploymentId(null)
      .build();
    brokerPartitions.insert(partition).await().indefinitely();
    final var verticleId = UUID.randomUUID().toString();
    final var claimedPartition = brokerPartitions.selectUnique(TopicPartitionPollingSession.CLAIM_PARTITION_STATEMENT, Map.of("verticleId", verticleId, "partitionId", partition.partitionId())).await().indefinitely();
    assertEquals(partition.partitionId(), claimedPartition.partitionId());
    assertEquals(verticleId, claimedPartition.deploymentId());
    assertEquals(true, claimedPartition.locked());
  }

  @Test
  void test_topic_partition_heartbeat() {
    final var partition = BrokerPartitionRecordBuilder.builder()
      .partitionId(UUID.randomUUID().toString())
      .baseRecord(BaseRecord.newRecord())
      .locked(true)
      .deploymentId("test-verticle")
      .build();
    brokerPartitions.insert(partition).await().indefinitely();
    final var claimedPartition1 = brokerPartitions.selectUnique(
        TopicPartitionPollingSession.HEART_BEAT_STATEMENT,
        Map.of("verticleId", partition.deploymentId(), "partitionId", partition.partitionId())
      )
      .await().indefinitely();
    final var claimedPartition2 = brokerPartitions.selectUnique(
      TopicPartitionPollingSession.HEART_BEAT_STATEMENT,
      Map.of("verticleId", partition.deploymentId(), "partitionId", partition.partitionId())
    ).await().indefinitely();
    assertNotEquals(claimedPartition1.baseRecord().lastUpdate(), claimedPartition2.baseRecord().lastUpdate());
  }


  MessageRecord topicMessage() {
    return new MessageRecord(
      UUID.randomUUID().toString(),
      null,
      null,
      null,
      MessageState.PUBLISHED,
      "test/message/payload",
      new JsonObject(),
      null,
      null,
      "partition-1",
      UUID.randomUUID().toString(),
      0,
      BaseRecord.newRecord()
    );
  }

  MessageRecord queueMessage() {
    return new MessageRecord(
      UUID.randomUUID().toString(),
      null,
      null,
      null,
      MessageState.PUBLISHED,
      "test/message/payload",
      new JsonObject(),
      null,
      null,
      "partition-1",
      UUID.randomUUID().toString(),
      0,
      BaseRecord.newRecord()
    );
  }

  ConsumerTransactionRecord consumerTransaction(Instant instant) {
    return new ConsumerTransactionRecord(
      UUID.randomUUID().toString(),
      UUID.randomUUID().toString(),
      new BaseRecord(
        "default",
        0,
        instant,
        Instant.now()
      )
    );
  }


}
