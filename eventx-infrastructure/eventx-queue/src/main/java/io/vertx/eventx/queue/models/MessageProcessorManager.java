package io.vertx.eventx.queue.models;

import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.queue.exceptions.ConsumerException;
import io.vertx.eventx.sql.exceptions.IntegrityContraintViolation;
import io.vertx.mutiny.core.Vertx;
import io.vertx.eventx.queue.MessageProcessor;
import io.vertx.eventx.queue.QueueTransactionManager;

import java.util.List;

import static io.vertx.eventx.queue.models.MessageState.*;


public record MessageProcessorManager(
  QueueConfiguration queueConfiguration,
  List<MessageProcessorWrapper> processorWrappers,
  QueueTransactionManager queueTransactionManager,
  Vertx vertx
) {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessorManager.class);
  public Uni<RawMessage> processMessage(RawMessage rawMessage) {
    final var processor = resolveProcessor(rawMessage);
    final var parsedMessage = parseMessage(rawMessage);
    return queueTransactionManager.transaction(
        parsedMessage, (msg, taskTransaction) -> {
          try {
            if (processor.blockingProcessor()) {
              return vertx.executeBlocking(process(parsedMessage, processor, taskTransaction)
                .onFailure().transform(ConsumerException::new)
              );
            }
            return process(parsedMessage, processor, taskTransaction)
              .onFailure().transform(ConsumerException::new);
          } catch (Exception exception) {
            throw new ConsumerException(exception);
          }
        }
      )
      .onItemOrFailure().transform(
        (avoid, throwable) -> {
          if (throwable != null && !(throwable instanceof IntegrityContraintViolation)) {
            return retryableFailure(queueConfiguration, rawMessage, throwable, processor);
          } else {
            return rawMessage.withState(PROCESSED);
          }
        }
      );
  }

  private Uni<Void> process(Message<?> message, MessageProcessor processor, QueueTransaction queueTransaction) {
    return processor.process(message.payload(), queueTransaction);
  }

  private MessageProcessor resolveProcessor(RawMessage messageRecord) {
    return processorWrappers.stream()
      .filter(processor -> processor.doesMessageMatch(messageRecord))
      .findFirst()
      .map(processor -> processor.resolveProcessor(messageRecord.tenant()))
      .orElseThrow();
  }

  private Message<?> parseMessage(RawMessage rawMessage) {
    final Class<?> tClass;
    try {
      tClass = Class.forName(rawMessage.payloadClass());
      return new Message<>(
        rawMessage.id(),
        rawMessage.tenant(),
        rawMessage.scheduled(),
        rawMessage.expiration(),
        rawMessage.priority(),
        rawMessage.payload().mapTo(tClass)
      );
    } catch (ClassNotFoundException e) {
      LOGGER.error("Unable to parse message {}" ,JsonObject.mapFrom(rawMessage), e);
      throw new ConsumerException(e);
    }
  }

  private <T> RawMessage retryableFailure(QueueConfiguration configuration, RawMessage messageRecord, Throwable throwable, MessageProcessor<T> processor) {
    MessageState failureState;
    if (processor.fatalExceptions().stream().anyMatch(f -> f.isAssignableFrom(throwable.getClass()))) {
      LOGGER.error("Fatal failure for message {} in processor {}", JsonObject.mapFrom(messageRecord).encodePrettily(), processor.getClass().getName(), throwable.getCause());
      failureState = FATAL_FAILURE;
    } else if (configuration.maxRetry() != null && messageRecord.retryCounter() + 1 > configuration.maxRetry()) {
      LOGGER.error("Retries exhausted for message {}  in processor {}", JsonObject.mapFrom(messageRecord).encodePrettily(), processor.getClass().getName(), throwable.getCause());
      failureState = RETRIES_EXHAUSTED;
    } else {
      LOGGER.error("Failure for message {} in processor {}", processor.getClass().getName(), JsonObject.mapFrom(messageRecord).encodePrettily(), throwable.getCause());
      failureState = RETRY;
    }
    return new RawMessage(
      messageRecord.id(),
      messageRecord.scheduled(),
      messageRecord.expiration(),
      messageRecord.priority(),
      messageRecord.retryCounter() + 1,
      failureState,
      messageRecord.payloadClass(),
      messageRecord.payload(),
      new JsonObject().put(processor.getClass().getName(), throwable.getMessage()),
      messageRecord.tenant()
    );
  }

}
