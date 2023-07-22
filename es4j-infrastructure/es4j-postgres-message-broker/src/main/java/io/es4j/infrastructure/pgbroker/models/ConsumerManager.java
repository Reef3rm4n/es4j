package io.es4j.infrastructure.pgbroker.models;


import io.es4j.infrastructure.pgbroker.ConsumerTransactionProvider;
import io.es4j.infrastructure.pgbroker.QueueConsumer;
import io.es4j.infrastructure.pgbroker.exceptions.DuplicateMessage;
import io.es4j.infrastructure.pgbroker.exceptions.ConsumerExeception;
import io.es4j.infrastructure.pgbroker.exceptions.InvalidProcessorException;
import io.es4j.infrastructure.pgbroker.exceptions.MessageParsingException;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public record ConsumerManager(
  PgBrokerConfiguration pgBrokerConfiguration,
  List<ConsumerWrap> consumerWraps,
  ConsumerTransactionProvider consumerTransactionProvider,
  Vertx vertx
) {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerManager.class);

  public Uni<RawMessage> consumeMessage(RawMessage rawMessage) {
    QueueConsumer processor;
    Message<?> parsedMessage;
    try {
      processor = resolveProcessor(rawMessage);
      parsedMessage = parseMessage(processor, rawMessage);
    } catch (Exception exception) {
      return Uni.createFrom().item(rawMessage.withState(MessageState.FATAL_FAILURE));
    }
    return consumerTransactionProvider.transaction(
        processor.getClass().getName(), parsedMessage, (msg, taskTransaction) -> {
          try {
            if (processor.blockingProcessor()) {
              return vertx.executeBlocking(process(parsedMessage, processor, taskTransaction)
                .onFailure().transform(ConsumerExeception::new)
              );
            }
            return process(parsedMessage, processor, taskTransaction)
              .onFailure().transform(ConsumerExeception::new);
          } catch (Exception exception) {
            throw new ConsumerExeception(exception);
          }
        }
      )
      .onItemOrFailure().transform(
        (avoid, throwable) -> {
          if (throwable != null) {
            if (throwable instanceof DuplicateMessage duplicateMessage) {
              LOGGER.debug("Duplicated message will be ignored {}  ", rawMessage);
              return rawMessage.withState(MessageState.PROCESSED);
            } else {
              return retryableFailure(pgBrokerConfiguration, rawMessage, throwable, processor);
            }
          } else {
            LOGGER.debug("Consumer {} has correctly processed the message {}  ", processor.getClass().getName(), rawMessage);
            return rawMessage.withState(MessageState.PROCESSED);
          }
        }
      );
  }

  private Uni<Void> process(Message<?> message, QueueConsumer processor, ConsumerTransaction consumerTransaction) {
    return processor.process(message.payload(), consumerTransaction);
  }

  private QueueConsumer resolveProcessor(RawMessage messageRecord) {
    return consumerWraps.stream()
      .filter(processor -> processor.doesMessageMatch(messageRecord))
      .findFirst()
      .map(processor -> processor.resolveProcessor(messageRecord.tenant()))
      .orElseThrow(() -> {
        LOGGER.error("Unable to find processor for message -> " + messageRecord.id());
        return new InvalidProcessorException();
      });
  }

  private Message<?> parseMessage(QueueConsumer processor, RawMessage rawMessage) {
    final Class<?> tClass;
    try {
      tClass = Class.forName(rawMessage.payloadClass());
      return new Message<>(
        rawMessage.id(),
        rawMessage.tenant(),
        String.valueOf(rawMessage.partitionId()),
        rawMessage.scheduled(),
        rawMessage.expiration(),
        rawMessage.priority(),
        processor.parse(rawMessage.payload(), tClass)
      );
    } catch (ClassNotFoundException e) {
      LOGGER.error("Could not find class for message -> " + rawMessage.id());
      throw new MessageParsingException();
    }
  }

  private <T> RawMessage retryableFailure(PgBrokerConfiguration configuration, RawMessage rawMessage, Throwable throwable, QueueConsumer<T> processor) {
    MessageState failureState;
    if (processor.fatalExceptions().stream().anyMatch(f -> f.isAssignableFrom(throwable.getCause().getClass()))) {
      LOGGER.error("Fatal failure {}, message will be sent to dead letter queue {} ", processor.getClass().getName(), rawMessage, throwable.getCause());
      failureState = MessageState.FATAL_FAILURE;
    } else if (rawMessage.retryCounter() + 1 > configuration.maxRetry()) {
      LOGGER.error("Retries exhausted for {} ", rawMessage, throwable.getCause());
      failureState = MessageState.RETRIES_EXHAUSTED;
    } else {
      LOGGER.error(String.format("Failure in task processor %s || Message %s will be queued for retry -> ", processor.getClass().getName(), rawMessage.id()), throwable.getCause());
      failureState = MessageState.RETRY;
    }
    return rawMessage.withFailure(failureState, throwable);
  }

}
