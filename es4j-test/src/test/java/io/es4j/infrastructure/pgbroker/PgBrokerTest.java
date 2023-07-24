package io.es4j.infrastructure.pgbroker;

import io.es4j.infrastructure.pgbroker.mappers.ConsumerFailureMapper;
import io.es4j.infrastructure.pgbroker.mappers.MessageMapper;
import io.es4j.infrastructure.pgbroker.mappers.MessageTransactionMapper;
import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.infrastructure.pgbroker.vertx.PgClientQueueProducer;
import io.es4j.infrastructure.pgbroker.vertx.PgClientTopicProducer;
import io.es4j.infrastructure.sql.SqlBootstrap;
import io.es4j.sql.Repository;
import io.es4j.sql.exceptions.NotFound;
import io.es4j.sql.models.QueryOptions;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public class PgBrokerTest {

  public static final SqlBootstrap BOOTSTRAP = new SqlBootstrap().setPostgres(true);
  final PgClientQueueProducer queueProducer = new PgClientQueueProducer(SqlBootstrap.REPOSITORY_HANDLER);
  final PgClientTopicProducer topicPublisher = new PgClientTopicProducer(SqlBootstrap.REPOSITORY_HANDLER);
  final Repository<ConsumerTransactionKey, ConsumerTransactionRecord, ConsumerTransactionQuery> consumerTransaction = new Repository<>(MessageTransactionMapper.INSTANCE, SqlBootstrap.REPOSITORY_HANDLER);
  final Repository<ConsumerFailureKey, ConsumerFailureRecord, ConsumerFailureQuery> consumerFailures = new Repository<>(ConsumerFailureMapper.INSTANCE, SqlBootstrap.REPOSITORY_HANDLER);
  final Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageBroker = new Repository<>(MessageMapper.INSTANCE, SqlBootstrap.REPOSITORY_HANDLER);

  @AfterAll
  static void destroy() {
    PgBroker.undeploy(SqlBootstrap.vertx).await().indefinitely();
    BOOTSTRAP.destroy();
  }

  @BeforeAll
  static void start() {
    BOOTSTRAP.start();
    PgBroker.deploy(BOOTSTRAP.CONFIGURATION, SqlBootstrap.vertx, 2).await().indefinitely();
  }

  @Test
  void test_queue_consumer() {
    final var sucessMessage = sucessQueueMessage();
    Assertions.assertDoesNotThrow(() -> send(sucessMessage));
    Assertions.assertDoesNotThrow(() -> pollConsumerTx(sucessMessage.messageId(), TestQueueConsumer.class.getName()));
    Assertions.assertThrows(NotFound.class, () -> checkConsumerFailure(sucessMessage.messageId(), TestQueueConsumer.class.getName()));
    Assertions.assertDoesNotThrow(() -> pollBrokerMessage(sucessMessage.messageId(), MessageState.CONSUMED));
  }

  @Test
  void test_queue_consumer_failure() {
    final var sucessMessage = failQueueMessage();
    Assertions.assertDoesNotThrow(() -> send(sucessMessage));
    Assertions.assertThrows(NotFound.class, () -> checkConsumerTx(sucessMessage.messageId(), TestQueueConsumer.class.getName()));
    Assertions.assertDoesNotThrow(() -> pollConsumerFailure(sucessMessage.messageId(), TestQueueConsumer.class.getName()));
    Assertions.assertDoesNotThrow(() -> pollBrokerMessage(sucessMessage.messageId(), MessageState.CONSUMED));
  }

  @Test
  void test_topic_consumer() {
    final var sucessMessage = sucessTopicMessage();
    Assertions.assertDoesNotThrow(() -> publish(sucessMessage));
    Assertions.assertDoesNotThrow(() -> pollConsumerTx(sucessMessage.messageId(), TestTopicSubscriber.class.getName()));
    Assertions.assertThrows(NotFound.class, () -> checkConsumerFailure(sucessMessage.messageId(), TestTopicSubscriber.class.getName()));
    Assertions.assertDoesNotThrow(() -> pollBrokerMessage(sucessMessage.messageId(), MessageState.CONSUMED));
  }

  @Test
  void test_topic_consumer_failure() {
    final var sucessMessage = failTopicMessage();
    Assertions.assertDoesNotThrow(() -> publish(sucessMessage));
    Assertions.assertThrows(NotFound.class, () -> checkConsumerTx(sucessMessage.messageId(), TestTopicSubscriber.class.getName()));
    Assertions.assertDoesNotThrow(() -> pollConsumerFailure(sucessMessage.messageId(), TestTopicSubscriber.class.getName()));
    Assertions.assertDoesNotThrow(() -> pollBrokerMessage(sucessMessage.messageId(), MessageState.CONSUMED));
  }


  public <T> MessageRecord pollBrokerMessage(String messageId, MessageState state) {
    return messageBroker.query(
        MessageRecordQueryBuilder.builder()
          .ids(List.of(messageId))
          .states(List.of(state))
          .options(QueryOptions.simple())
          .build()
      )
      .onFailure(NotFound.class).retry().withBackOff(Duration.ofMillis(100)).atMost(10)
      .map(msg -> msg.stream().findFirst().orElseThrow())
      .await().indefinitely();
  }

  public <T> void pollConsumerTx(String messageId, String consumer) {
    consumerTransaction.selectByKey(
        new ConsumerTransactionKey(messageId, consumer)
      )
      .onFailure(NotFound.class).retry().withBackOff(Duration.ofMillis(100)).atMost(10)
      .await().indefinitely();
  }

  public <T> void checkConsumerTx(String messageId, String consumer) {
    consumerTransaction.selectByKey(
        new ConsumerTransactionKey(messageId, consumer)
      )
      .await().indefinitely();
  }

  public <T> void pollConsumerFailure(String messageId, String consumer) {
    consumerFailures.selectByKey(new ConsumerFailureKey(messageId, consumer))
      .onFailure(NotFound.class).retry().withBackOff(Duration.ofMillis(100)).atMost(10)
      .await().indefinitely();
  }

  public <T> void checkConsumerFailure(String messageId, String consumer) {
    consumerFailures.selectByKey(new ConsumerFailureKey(messageId, consumer))
      .await().indefinitely();
  }

  private List<ConsumerTransactionRecord> pollTxs(List<String> messagesIds) {
    return Multi.createBy().repeating().uni(
        () -> consumerTransaction.query(
            ConsumerTransactionQueryBuilder.builder()
              .ids(messagesIds)
              .options(QueryOptions.simple())
              .build()
          )
          .onFailure().recoverWithNull()
      )
      .withDelay(Duration.ofMillis(250))
      .atMost(20)
      .collect().last()
      .map(Objects::requireNonNull)
      .await().indefinitely();
  }

  private List<ConsumerFailureRecord> pollConsumerFailures(List<String> messageIds) {
    return Multi.createBy().repeating().uni(
        () -> consumerFailures.query(
            ConsumerFailureQueryBuilder.builder()
              .ids(messageIds)
              .options(QueryOptions.simple())
              .build()
          )
          .onFailure().recoverWithNull()
      )
      .withDelay(Duration.ofMillis(250))
      .atMost(20)
      .collect().last()
      .map(Objects::requireNonNull)
      .await().indefinitely();
  }

  private void publish(List<TopicMessage<Object>> messages) {
    topicPublisher.publish(messages).await().indefinitely();
  }

  private void publish(TopicMessage<Object> fakeMessage) {
    topicPublisher.publish(fakeMessage).await().indefinitely();
  }

  private void send(QueueMessage<Object> fakeMessage) {
    queueProducer.publish(fakeMessage).await().indefinitely();
  }

  private static TopicMessage<Object> sucessTopicMessage() {
    return TopicMessageBuilder.builder()
      .payload(new TestTopicPayload("data", false))
      .build();
  }

  private static TopicMessage<Object> failTopicMessage() {
    return TopicMessageBuilder.builder()
      .payload(new TestTopicPayload("data", true))
      .build();
  }

  private static QueueMessage<Object> sucessQueueMessage() {
    return QueueMessageBuilder.builder()
      .payload(new TestQueuePayload("data", false))
      .build();
  }

  private static QueueMessage<Object> failQueueMessage() {
    return QueueMessageBuilder.builder()
      .payload(new TestQueuePayload("data", true))
      .build();
  }


}
