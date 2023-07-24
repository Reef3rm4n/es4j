package io.es4j.infrastructure.pgbroker.core;

import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.sql.Repository;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.FixedDemandPacer;
import io.smallrye.mutiny.tuples.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

import static java.util.stream.Collectors.groupingBy;

public class MessageRouter {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageRouter.class);
  private final Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageBroker;
  private final Repository<ConsumerFailureKey, ConsumerFailureRecord, ConsumerFailureQuery> consumerFailures;
  private final ConsumerRouter consumerRouter;

  public MessageRouter(
    final ConsumerRouter consumerRouter,
    final Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageBroker,
    final Repository<ConsumerFailureKey, ConsumerFailureRecord, ConsumerFailureQuery> consumerFailures
  ) {
    this.consumerRouter = consumerRouter;
    this.messageBroker = messageBroker;
    this.consumerFailures = consumerFailures;
  }


  public Uni<Void> routeTopicPartition(List<MessageRecord> messageRecords) {
    return startPacedStream(splitOnPartitionKey(messageRecords).entrySet())
      .onItem().transformToUniAndMerge(
        partitionStream -> {
          LOGGER.debug("Processing partition stream {} -> {}", partitionStream.getKey(), partitionStream.getValue());
          return processTopicPartitionStream(partitionStream.getValue());
        }
      )
      .collect().asList()
      .flatMap(lists -> {
          final var rawMessages = lists.stream().flatMap(List::stream).toList();
          final var messages = rawMessages.stream().map(Tuple2::getItem1).map(MessageRecord::from).toList();
          final var failures = rawMessages.stream().map(Tuple2::getItem2).flatMap(List::stream).toList();
          return Uni.join().all(updateConsumedMessages(messages), persistFailures(failures)).andFailFast();
        }
      )
      .replaceWithVoid();
  }


  public Uni<Void> routeQueues(List<MessageRecord> concurrentMessages) {
    return startPacedStream(concurrentMessages).onItem()
      .transformToUniAndMerge(messageRecord -> {
        if (Objects.nonNull(messageRecord.expiration()) && Instant.now().isAfter(messageRecord.expiration())) {
          return Uni.createFrom().item(Tuple2.<RawMessage, Optional<ConsumerFailureRecord>>of(parseMessage(messageRecord).withState(MessageState.EXPIRED), Optional.empty()));
        }
        return consumerRouter.routeQueue(parseMessage(messageRecord));
      })
      .collect().asList()
      .flatMap(rawMessages -> {
          final var messages = rawMessages.stream().map(Tuple2::getItem1).map(MessageRecord::from).toList();
          final var failures = rawMessages.stream().map(Tuple2::getItem2).flatMap(Optional::stream).toList();
          return Uni.join().all(updateConsumedMessages(messages), persistFailures(failures)).andFailFast();
        }
      )
      .replaceWithVoid();
  }

  private Multi<MessageRecord> startPacedStream(Iterable<MessageRecord> messageRecords) {
    if (consumerRouter.brokerConfiguration().consumerConcurrency() != null) {
      final var pacer = new FixedDemandPacer(consumerRouter.brokerConfiguration().consumerConcurrency(), consumerRouter.brokerConfiguration().consumerThrottle());
      return Multi.createFrom().iterable(messageRecords).paceDemand().using(pacer);
    }
    return Multi.createFrom().iterable(messageRecords);
  }

  private Multi<Map.Entry<String, List<RawMessage>>> startPacedStream(Set<Map.Entry<String, List<RawMessage>>> partitions) {
    if (consumerRouter.brokerConfiguration().consumerConcurrency() != null) {
      final var pacer = new FixedDemandPacer(consumerRouter.brokerConfiguration().consumerConcurrency(), consumerRouter.brokerConfiguration().consumerThrottle());
      return Multi.createFrom().iterable(partitions).paceDemand().using(pacer);
    }
    return Multi.createFrom().iterable(partitions);
  }

  private Uni<List<Tuple2<RawMessage, List<ConsumerFailureRecord>>>> processTopicPartitionStream(List<RawMessage> partitionedMessages) {
    final var messagesToProcess = fillStack(partitionedMessages);
    LOGGER.debug("Processing partition messages {}", messagesToProcess);
    return Multi.createBy().repeating().supplier(messagesToProcess::pop)
      .whilst((avoid) -> !messagesToProcess.isEmpty())
      .onItem().transformToUniAndConcatenate(consumerRouter::fanOut)
      .collect().asList();
  }

  private static Stack<RawMessage> fillStack(List<RawMessage> partitionedMessages) {
    final var messagesToProcess = new Stack<RawMessage>();
    partitionedMessages.stream().sorted(Comparator.comparing(RawMessage::messageSequence))
      .forEach(messagesToProcess::push);
    return messagesToProcess;
  }


  private Map<String, List<RawMessage>> splitOnPartitionKey(List<MessageRecord> messageRecords) {
    return messageRecords.stream()
      .map(this::parseMessage)
      .collect(groupingBy(RawMessage::partitionKey));
  }

  private RawMessage parseMessage(MessageRecord messageRecord) {
    return new RawMessage(messageRecord.messageId(),
      messageRecord.scheduled(),
      messageRecord.expiration(),
      messageRecord.priority(),
      messageRecord.messageState(),
      messageRecord.messageAddress(),
      messageRecord.payload(),
      messageRecord.baseRecord().tenant(),
      messageRecord.messageSequence(),
      messageRecord.partitionId(),
      messageRecord.partitionKey(),
      messageRecord.schemaVersion()
    );
  }

  private Uni<Void> updateConsumedMessages(List<MessageRecord> messages) {
    if (!messages.isEmpty()) {
      LOGGER.debug("Updating messages {}", messages);
      return messageBroker.updateByKeyBatch(messages);
    }
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> persistFailures(List<ConsumerFailureRecord> consumerFailureRecords) {
    if (!consumerFailureRecords.isEmpty()) {
      LOGGER.debug("Persisting failures {}", consumerFailureRecords);
      return this.consumerFailures.insertBatch(consumerFailureRecords);
    }
    return Uni.createFrom().voidItem();
  }
}
