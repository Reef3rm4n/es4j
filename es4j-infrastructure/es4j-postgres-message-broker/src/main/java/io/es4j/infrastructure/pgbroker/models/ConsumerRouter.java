package io.es4j.infrastructure.pgbroker.models;


import io.es4j.infrastructure.pgbroker.ConsumerTransactionProvider;
import io.es4j.infrastructure.pgbroker.exceptions.DuplicateMessage;
import io.es4j.infrastructure.pgbroker.exceptions.InvalidProcessorException;
import io.es4j.sql.models.BaseRecord;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;


public record ConsumerRouter(
  BrokerConfiguration brokerConfiguration,
  List<TopicSubscriberWrapper> topicConsumers,
  List<QueueConsumerWrapper> queueConsumers,
  ConsumerTransactionProvider consumerTransactionProvider,
  Vertx vertx
) {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRouter.class);

  public Uni<Tuple2<RawMessage, List<ConsumerFailureRecord>>> fanOut(RawMessage rawMessage) {
    final var failures = new ArrayList<ConsumerFailureRecord>();
    return Multi.createFrom().iterable(findConsumers(rawMessage))
      .onItem().transformToUniAndMerge(
        topicSubscriberWrapper -> consumerTransactionProvider.transaction(
          topicSubscriberWrapper.consumer().getClass().getName(), rawMessage, (msg, consumerTransaction) -> {
            if (topicSubscriberWrapper.consumer().blocking()) {
              return vertx.executeBlocking(process(rawMessage, topicSubscriberWrapper, consumerTransaction, failures));
            }
            return process(rawMessage, topicSubscriberWrapper, consumerTransaction, failures);
          }
        )
      )
      .collect().asList()
      .map(consumerFailures -> Tuple2.of(rawMessage.withState(MessageState.CONSUMED), failures));
  }

  private <T> Uni<Void> process(RawMessage rawMessage, TopicSubscriberWrapper<T> topicSubscriber, ConsumerTransaction taskTransaction, List<ConsumerFailureRecord> consumerFailureRecords) {
    return topicSubscriber.consume(rawMessage, taskTransaction)
      .onFailure(throwable -> topicSubscriber.consumer().retryOn().stream().anyMatch(t -> t.isAssignableFrom(throwable.getClass())))
      .retry().withBackOff(topicSubscriber.consumer().retryBackOff()).atMost(topicSubscriber.consumer().numberOfAttempts())
      .onFailure().invoke(throwable -> LOGGER.error("Topic subscriber {} failed to process message {}", topicSubscriber.consumer().getClass().getSimpleName(), rawMessage, throwable))
      .onItemOrFailure().transform((item, throwable) -> {
        if (Objects.nonNull(throwable)) {
          return consumerFailureRecords.add(parseConsumerFailure(topicSubscriber.consumer().getClass().getName(), rawMessage.messageId(), throwable));
        }
        return Void.TYPE;
      })
      .replaceWithVoid();
  }

  private <T> Uni<Tuple2<RawMessage, Optional<ConsumerFailureRecord>>> process(RawMessage rawMessage, QueueConsumerWrapper<T> queueConsumer, ConsumerTransaction taskTransaction) {
    return queueConsumer.consume(rawMessage, taskTransaction)
      .onFailure(throwable -> queueConsumer.consumer().retryOn().stream().anyMatch(t -> t.isAssignableFrom(throwable.getClass())))
      .retry().withBackOff(queueConsumer.consumer().retryBackOff()).atMost(queueConsumer.consumer().numberOfAttempts())
      .onItemOrFailure().transform(
        (avoid, throwable) -> {
          if (throwable != null && !(throwable instanceof DuplicateMessage)) {
            LOGGER.error("{} failed for message -> {} ",queueConsumer.consumer().getClass().getName(), rawMessage, throwable);
            return Tuple2.of(rawMessage.withState(MessageState.CONSUMED), Optional.of(parseConsumerFailure(queueConsumer.consumer().getClass().getName(), rawMessage.messageId(), throwable)));
          }
          LOGGER.debug("{} consumed message -> {}", queueConsumer.consumer().getClass().getName(), rawMessage);
          return Tuple2.of(rawMessage.withState(MessageState.CONSUMED), Optional.empty());
        }
      );
  }

  private ConsumerFailureRecord parseConsumerFailure(String consumer, String messageId, Throwable throwable) {
    return ConsumerFailureRecordBuilder.builder()
      .messageId(messageId)
      .consumer(consumer)
      .error(
        new JsonObject()
          .put("message", throwable.getMessage())
          .put("localMessage", throwable.getLocalizedMessage())
          .put("causeMessage", Objects.nonNull(throwable.getCause()) ? throwable.getCause().getMessage() : null)
          .put("causeLocalMessage", Objects.nonNull(throwable.getCause()) ? throwable.getCause().getLocalizedMessage() : null)
      )
      .baseRecord(BaseRecord.newRecord())
      .build();
  }


  private List<TopicSubscriberWrapper> findConsumers(RawMessage messageRecord) {
    return topicConsumers.stream()
      .filter(processor -> processor.match(messageRecord))
      .toList();
  }


  public Uni<Tuple2<RawMessage, Optional<ConsumerFailureRecord>>> routeQueue(RawMessage rawMessage) {
    final var queueConsumer = resolveQueueConsumer(rawMessage);
    return consumerTransactionProvider.transaction(
        queueConsumer.consumer().getClass().getName(), rawMessage, (msg, taskTransaction) -> {
          if (queueConsumer.consumer().blocking()) {
            return vertx.executeBlocking(process(rawMessage, queueConsumer, taskTransaction));
          }
          return process(rawMessage, queueConsumer, taskTransaction);
        }
      );
  }

  private <T> QueueConsumerWrapper<T> resolveQueueConsumer(RawMessage messageRecord) {
    return queueConsumers.stream()
      .filter(processor -> processor.isMatch(messageRecord))
      .findFirst()
      .orElseThrow(() -> {
        LOGGER.error("Unable to find processor for message -> {}", messageRecord);
        return new InvalidProcessorException();
      });
  }


}
