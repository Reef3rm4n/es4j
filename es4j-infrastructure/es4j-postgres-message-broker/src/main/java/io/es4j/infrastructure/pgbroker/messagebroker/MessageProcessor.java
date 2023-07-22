package io.es4j.infrastructure.pgbroker.messagebroker;

import io.es4j.infrastructure.pgbroker.exceptions.InterruptMessageStream;
import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.sql.Repository;
import io.es4j.sql.exceptions.NotFound;
import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.QueryOptions;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.FixedDemandPacer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.util.stream.Collectors.groupingBy;

public class MessageProcessor {
  private static final EnumSet<MessageState> DEAD_LETTER_QUEUE_STATES = EnumSet.of(MessageState.PARKED, MessageState.FATAL_FAILURE, MessageState.RETRIES_EXHAUSTED, MessageState.EXPIRED);
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessor.class);
  private final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue;
  private final Repository<DeadLetterKey, DeadLetterRecord, MessageRecordQuery> deadLetterQueue;
  private final ConsumerManager consumerManager;

  public MessageProcessor(
    final ConsumerManager consumerManager,
    final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue,
    final Repository<DeadLetterKey, DeadLetterRecord, MessageRecordQuery> deadLetterQueue
  ) {
    this.consumerManager = consumerManager;
    this.messageQueue = messageQueue;
    this.deadLetterQueue = deadLetterQueue;
  }


  public Uni<Void> processPartitions(List<MessageRecord> messageRecords) {
    if (messageRecords.stream().anyMatch(m -> Objects.isNull(m.partitionId()) || Objects.equals(m.partitionId(), "none"))) {
      throw new IllegalArgumentException("Message missing partitionId");
    }
    if (messageRecords.stream().anyMatch(m -> Objects.isNull(m.partitionKey()))) {
      throw new IllegalArgumentException("Message missing partitionKey");
    }
    return startPacedStream(splitOnPartitionKey(messageRecords).entrySet())
      .onItem().transformToUniAndMerge(
        partitionStream -> searchPartitionKeyInDeadletterQueue(partitionStream.getKey())
          .flatMap(deadLetterRecords -> {
              if (deadLetterRecords.isEmpty()) {
                LOGGER.debug("Processing messages {}", partitionStream.getValue());
                return processPartitionStream(partitionStream.getValue());
              }
              LOGGER.debug("Parking messages {}", partitionStream.getValue());
              return Uni.createFrom().item(partitionStream.getValue().stream().map(RawMessage::park).toList());
            }
          )
      )
      .collect().asList()
      .map(lists -> lists.stream().flatMap(List::stream).toList())
      .flatMap(rawMessages -> persistResults(rawMessages.stream().map(MessageRecord::from).toList()));
  }


  public Uni<Void> processConcurrent(List<MessageRecord> concurrentMessages) {
    if (concurrentMessages.stream().anyMatch(m -> !Objects.equals(m.partitionId(), "none"))) {
      throw new IllegalArgumentException("Message with partition");
    }
    return startPacedStream(concurrentMessages).onItem()
      .transformToUniAndMerge(messageRecord -> consumerManager.consumeMessage(parseRecord(messageRecord)))
      .collect().asList()
      .flatMap(rawMessages -> persistResults(rawMessages.stream().map(MessageRecord::from).toList()));
  }


  private Uni<List<DeadLetterRecord>> searchPartitionKeyInDeadletterQueue(String partitionKey) {
    return deadLetterQueue.query(deadLetterQuery(partitionKey))
      .onFailure(NotFound.class)
      .recoverWithItem(Collections.emptyList());
  }

  private Multi<MessageRecord> startPacedStream(Iterable<MessageRecord> messageRecords) {
    if (consumerManager.pgBrokerConfiguration().concurrency() != null) {
      final var pacer = new FixedDemandPacer(consumerManager.pgBrokerConfiguration().concurrency(), consumerManager.pgBrokerConfiguration().throttle());
      return Multi.createFrom().iterable(messageRecords).paceDemand().using(pacer);
    }
    return Multi.createFrom().iterable(messageRecords);
  }

  private Multi<Map.Entry<String, List<RawMessage>>> startPacedStream(Set<Map.Entry<String, List<RawMessage>>> partitions) {
    if (consumerManager.pgBrokerConfiguration().concurrency() != null) {
      final var pacer = new FixedDemandPacer(consumerManager.pgBrokerConfiguration().concurrency(), consumerManager.pgBrokerConfiguration().throttle());
      return Multi.createFrom().iterable(partitions).paceDemand().using(pacer);
    }
    return Multi.createFrom().iterable(partitions);
  }

  private Uni<List<RawMessage>> processPartitionStream(List<RawMessage> partitionedMessages) {
    final var messagesToProcess = fillStack(partitionedMessages);
    LOGGER.debug("Processing partition messages {}", messagesToProcess);
    final var processedMessages = new ArrayList<RawMessage>(partitionedMessages.size());
    return Multi.createBy().repeating().supplier(messagesToProcess::pop)
      .whilst((avoid) -> !messagesToProcess.isEmpty())
      .onItem().transformToUniAndConcatenate(
        rawMessage -> consumerManager.consumeMessage(rawMessage)
        .map(resultingMessage -> handleProcessResult(messagesToProcess, processedMessages, resultingMessage))
      )
      .collect().asList()
      .onItemOrFailure()
      .transform((i, f) -> processedMessages);
  }

  private static Class<Void> handleProcessResult(Stack<RawMessage> messagesToProcess, ArrayList<RawMessage> processedMessages, RawMessage resultingMessage) {
    processedMessages.add(resultingMessage);
    if (resultingMessage.messageState() != MessageState.PROCESSED) {
      LOGGER.debug("Message failed parking the rest of the stream {}", messagesToProcess);
      messagesToProcess.forEach(unprocessedMessage -> processedMessages.add(unprocessedMessage.park()));
      throw new InterruptMessageStream();
    }
    return Void.TYPE;
  }

  private static Stack<RawMessage> fillStack(List<RawMessage> partitionedMessages) {
    final var messagesToProcess = new Stack<RawMessage>();
    partitionedMessages.stream().sorted(Comparator.comparing(RawMessage::messageSequence))
      .forEach(messagesToProcess::push);
    return messagesToProcess;
  }

  private static MessageRecordQuery deadLetterQuery(String partitionKey) {
    return MessageRecordQueryBuilder.builder()
      .partitionKey(partitionKey)
      .options(QueryOptions.simple())
      .build();
  }


  private Map<String, List<RawMessage>> splitOnPartitionKey(List<MessageRecord> messageRecords) {
    return messageRecords.stream()
      .map(this::parseRecord)
      .collect(groupingBy(RawMessage::partitionKey));
  }

  private RawMessage parseRecord(MessageRecord messageRecord) {
    return new RawMessage(messageRecord.id(),
      messageRecord.scheduled(),
      messageRecord.expiration(),
      messageRecord.priority(),
      messageRecord.retryCounter(),
      messageRecord.messageState(),
      messageRecord.payloadClass(),
      messageRecord.payload(),
      messageRecord.failedProcessors(),
      messageRecord.baseRecord().tenant(),
      messageRecord.messageSequence(),
      messageRecord.partitionId(),
      messageRecord.partitionKey());
  }

  private Uni<Void> persistResults(List<MessageRecord> messages) {
    LOGGER.debug("Persisting results for {}", messages);
    return Uni.join().all(ack(messages), nack(messages)).andFailFast().replaceWithVoid();
  }

  private Uni<Void> nack(List<MessageRecord> messages) {
    final var messagesToRequeue = messages.stream()
      .filter(entry -> entry.messageState() == MessageState.RETRY)
      .toList();
    if (!messagesToRequeue.isEmpty()) {
      LOGGER.debug("re-queuing unhandled messages {}", messagesToRequeue.stream().map(MessageRecord::id).toList());
      return messageQueue.updateByKeyBatch(messagesToRequeue);
    }
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> ack(List<MessageRecord> messages) {
    final var unis = new ArrayList<Uni<Void>>();
    final var messagesToDrop = messages.stream()
      .filter(message -> message.messageState() == MessageState.PROCESSED || DEAD_LETTER_QUEUE_STATES.contains(message.messageState()))
      .collect(groupingBy(msg -> msg.baseRecord().tenant()));
    final var queries = messagesToDrop.entrySet().stream()
      .map(this::messageDropQuery)
      .toList();
    final var messagesToMoveToDeadLetter = messages.stream()
      .filter(message -> DEAD_LETTER_QUEUE_STATES.contains(message.messageState()))
      .map(MessageProcessor::parseDeadLetter)
      .toList();
    if (!queries.isEmpty()) {
      LOGGER.debug("Dropping messages {}", messagesToDrop);
      unis.add(
        Multi.createFrom().iterable(queries)
          .onItem().transformToUniAndMerge(messageQueue::deleteQuery)
          .collect().asList().replaceWithVoid()
      );
    }
    if (!messagesToMoveToDeadLetter.isEmpty()) {
      LOGGER.debug("Moving messages to dead-letter {}", messagesToMoveToDeadLetter);
      unis.add(deadLetterQueue.insertBatch(messagesToMoveToDeadLetter));
    }
    if (!unis.isEmpty()) {
      return Uni.join().all(unis).andCollectFailures().replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }

  private static DeadLetterRecord parseDeadLetter(MessageRecord messageRecord) {
    return new DeadLetterRecord(
      messageRecord.id(),
      messageRecord.scheduled(),
      messageRecord.expiration(),
      messageRecord.priority(),
      messageRecord.retryCounter(),
      messageRecord.messageState(),
      messageRecord.payloadClass(),
      messageRecord.payload(),
      messageRecord.failedProcessors(),
      messageRecord.verticleId(),
     messageRecord.partitionId(),
     messageRecord.partitionKey(),
      BaseRecord.newRecord(messageRecord.baseRecord().tenant())
    );
  }

  private MessageRecordQuery messageDropQuery(Map.Entry<String, List<MessageRecord>> entry) {
    return MessageRecordQueryBuilder.builder()
      .ids(
        entry.getValue().stream()
          .map(MessageRecord::id)
          .toList()
      )
      .options(QueryOptions.simple(entry.getKey()))
      .build();
  }
}
