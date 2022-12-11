package io.vertx.skeleton.ccp.producers;

import io.vertx.skeleton.ccp.QueueConsumerVerticle;
import io.vertx.skeleton.ccp.models.EvMessage;
import io.vertx.skeleton.ccp.models.EvMessageBatch;
import io.vertx.skeleton.ccp.models.Message;
import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.VertxServiceException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.List;
import java.util.function.Function;

import static io.vertx.skeleton.models.RequestMetadata.X_TXT_ID;

public class EventbusProducer<T> {


  private final Vertx vertx;
  private final String queueName;

  private final Logger logger;


  public EventbusProducer(Vertx vertx, String queueName) {
    this.vertx = vertx;
    this.queueName = queueName;
    this.logger = LoggerFactory.getLogger(queueName);
  }

  public Uni<Void> enqueue(Message<T> entry) {
    logger.info("Producing entry -> " + entry.id());
    final var message = new EvMessageBatch(List.of(new EvMessage(
        entry.id(),
        entry.tenant(),
        entry.scheduled(),
        entry.expiration(),
        entry.priority(),
        JsonObject.mapFrom(entry.payload()).getMap()
      )
    )
    );
    return vertx.eventBus().request(
        QueueConsumerVerticle.class.getName(),
        message,
        new DeliveryOptions()
          .setSendTimeout(2000)
          .setLocalOnly(true)
          .addHeader("queue", queueName)
          .addHeader(X_TXT_ID, entry.id())
      )
      .onFailure().transform(parseThrowable())
      .replaceWithVoid();
  }

  public void enqueueAndForget(Message<T> entry) {
    logger.info("Producing entry -> " + entry.id());
    final var message = new EvMessageBatch(List.of(new EvMessage(
        entry.id(),
        entry.tenant(),
        entry.scheduled(),
        entry.expiration(),
        entry.priority(),
        JsonObject.mapFrom(entry.payload()).getMap()
      )
    )
    );

    vertx.eventBus().requestAndForget(
      QueueConsumerVerticle.class.getName(),
      message,
      new DeliveryOptions()
        .setSendTimeout(2000)
        .setLocalOnly(true)
        .addHeader("queue", queueName)
        .addHeader(X_TXT_ID, entry.id()
        )
    );
  }

  public Uni<Void> enqueue(List<Message<T>> entries) {
    logger.debug("Producing entries -> " + entries.stream().map(Message::id).toList());
    final var queueEntries = entries.stream().map(
      entry -> new EvMessage(
        entry.id(),
        entry.tenant(),
        entry.scheduled(),
        entry.expiration(),
        entry.priority(),
        JsonObject.mapFrom(entry.payload()).getMap()
      )
    ).toList();
    return vertx.eventBus().request(
        QueueConsumerVerticle.class.getName(),
        new EvMessageBatch(queueEntries),
        new DeliveryOptions()
          .setLocalOnly(true)
          .addHeader("queue", queueName)
          .setSendTimeout(2000)
      )
      .onFailure().transform(parseThrowable())
      .replaceWithVoid();
  }

  private Function<Throwable, Throwable> parseThrowable() {
    return Unchecked.function(throwable -> {
        if (throwable instanceof ReplyException replyException) {
          if (replyException.failureType() == ReplyFailure.RECIPIENT_FAILURE) {
            throw new VertxServiceException(
              new JsonObject(replyException.getMessage()).mapTo(Error.class)
            );
          }
          logger.info("Consumer produced unexpected error ->", replyException);
          throw new VertxServiceException(
            replyException.getMessage(),
            replyException.failureType().name(),
            400
          );
        } else {
          logger.error("Unexpected error in bus proxy", throwable);
          throw new VertxServiceException(
            throwable.getMessage(),
            throwable.getLocalizedMessage(),
            500
          );
        }
      }
    );
  }

  public void enqueueAndForget(List<Message<T>> entries) {
    logger.debug("Producing entries -> " + entries.stream().map(Message::id).toList());
    final var queueEntries = entries.stream().map(
      entry -> new EvMessage(
        entry.id(),
        entry.tenant(),
        entry.scheduled(),
        entry.expiration(),
        entry.priority(),
        JsonObject.mapFrom(entry.payload()).getMap()
      )
    ).toList();
    vertx.eventBus().requestAndForget(
      QueueConsumerVerticle.class.getName(),
      new EvMessageBatch(queueEntries),
      new DeliveryOptions()
        .setLocalOnly(true)
        .addHeader("queue", queueName)
        .setSendTimeout(2000)
    );
  }

}
