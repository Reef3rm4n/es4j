package io.vertx.eventx.queue.models;

import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.queue.exceptions.MessageProcessorException;
import io.vertx.eventx.sql.exceptions.IntegrityContraintViolation;
import io.vertx.mutiny.core.Vertx;
import io.vertx.eventx.queue.MessageProcessor;
import io.vertx.eventx.queue.TransactionManager;

import java.util.List;

import static io.vertx.eventx.queue.models.MessageState.*;


public record MessageProcessorManager(
  QueueConfiguration queueConfiguration,
  List<MessageProcessorWrapper> processorWrappers,
  TransactionManager transactionManager,
  Vertx vertx
) {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessorManager.class);

  // todo add circuit-breakers to processors.
  public Uni<RawMessage> processMessage(RawMessage rawMessage) {
    final var processor = resolveProcessor(rawMessage);
    final var parsedMessage = parseMessage(rawMessage);
    return transactionManager.transaction(
        parsedMessage, (msg, taskTransaction) -> {
          try {
            if (processor.blockingProcessor()) {
              return vertx.executeBlocking(process(parsedMessage, processor, taskTransaction)
                .onFailure().transform(MessageProcessorException::new)
              );
            }
            return process(parsedMessage, processor, taskTransaction)
              .onFailure().transform(MessageProcessorException::new);
          } catch (Exception exception) {
            throw new MessageProcessorException(exception);
          }
        }
      )
      .onItemOrFailure().transform(
        (avoid, throwable) -> {
          if (throwable != null) {
            if (throwable instanceof IntegrityContraintViolation) {
              return rawMessage.withState(PROCESSED);
            } else {
              return retryableFailure(queueConfiguration, rawMessage, throwable, processor);
            }
          } else {
            LOGGER.info(processor.getClass().getName() + " has correctly processed the message -> " + rawMessage.id());
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
      throw new MessageProcessorException(e);
    }
  }

  private <T> RawMessage retryableFailure(QueueConfiguration configuration, RawMessage messageRecord, Throwable throwable, MessageProcessor<T> processor) {
    MessageState failureState;
    if (processor.fatalExceptions().stream().anyMatch(f -> f.isAssignableFrom(throwable.getClass()))) {
      LOGGER.error("Fatal failure in processor" + processor.getClass().getName() + ", ccp will drop message -> " + messageRecord.id(), throwable.getCause());
      failureState = FATAL_FAILURE;
    } else if (configuration.maxRetry() != null && messageRecord.retryCounter() + 1 > configuration.maxRetry()) {
      LOGGER.error("Retries exhausted for message -> " + messageRecord.id(), throwable.getCause());
      failureState = RETRIES_EXHAUSTED;
    } else {
      LOGGER.error("Failure in processor" + processor.getClass().getName() + ",  ccp will requeue message for retry -> " + messageRecord.id(), throwable.getCause());
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


  private <T> RawMessage circuitBreakerOpen(RawMessage messageRecord, MessageProcessor<T> processor) {
    LOGGER.error("Circuit-Breaker open for task processor " + processor.getClass().getName() + ", re-queueing message for retry -> " + messageRecord.id());
    return new RawMessage(
      messageRecord.id(),
      messageRecord.scheduled(),
      messageRecord.expiration(),
      messageRecord.priority(),
      messageRecord.retryCounter(),
      RETRY,
      messageRecord.payloadClass(),
      messageRecord.payload(),
      new JsonObject().put(processor.getClass().getName(), "circuit breaker open"),
      messageRecord.tenant()
    );
  }
}
