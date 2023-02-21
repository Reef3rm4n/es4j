package io.vertx.skeleton.ccp.producers;

import io.vertx.skeleton.ccp.QueueMessageProcessor;
import io.vertx.skeleton.ccp.models.Message;
import com.rabbitmq.client.AMQP;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;

public class RabbitMQProducer<T> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQProducer.class);

  private final RabbitMQClient rabbitMQClient;
  private final Class<? extends QueueMessageProcessor<T, ?>> queueClass;
  private final Vertx vertx;

  public RabbitMQProducer(
    final RabbitMQClient rabbitMQClient,
    final Class<? extends QueueMessageProcessor<T, ?>> queueClass,
    Vertx vertx
  ) {
    this.rabbitMQClient = rabbitMQClient;
    this.queueClass = queueClass;
    this.vertx = vertx;
  }


  public Uni<Void> enqueueAndConfirm(Message<T> message) {
    return publishAndWaitForConfirmation(message, parseProps(message));
  }

  public void enqueueConfirmAndForget(final Message<T> message) {
    publishAndWaitForConfirmation(message, parseProps(message))
      .subscribe()
      .with(
        avoid -> LOGGER.info("Message published to " + queueClass.getName() + " entry -> " + message.id()),
        throwable -> LOGGER.error("Unable to publish message to " + queueClass.getName() + " -> " + JsonObject.mapFrom(message).encodePrettily(), throwable)
      );
  }

  public Uni<Void> enqueueAndConfirm(List<Message<T>> entries) {
    return Multi.createFrom().iterable(entries)
      .onItem().transformToUniAndMerge(message -> publishAndWaitForConfirmation(message, parseProps(message)))
      .collect().asList()
      .replaceWithVoid();
  }

  public void enqueueConfirmAndForget(List<Message<T>> entries) {
    Multi.createFrom().iterable(entries)
      .onItem().transformToUniAndMerge(message -> publishAndWaitForConfirmation(message, parseProps(message)))
      .collect().asList()
      .replaceWithVoid()
      .subscribe()
      .with(
        avoid -> LOGGER.info("Messages published to " + queueClass.getName() + " entry ! "),
        throwable -> LOGGER.error("Unable to publish messages to " + queueClass.getName() + " -> " + new JsonArray(entries).encodePrettily(), throwable)
      );
  }

  public Uni<Void> enqueue(List<Message<T>> entries) {
    return Multi.createFrom().iterable(entries)
      .onItem().transformToUniAndMerge(message -> publish(message, parseProps(message)))
      .collect().asList()
      .replaceWithVoid();
  }

  public void enqueueAndForget(List<Message<T>> entries) {
    Multi.createFrom().iterable(entries)
      .onItem().transformToUniAndMerge(message -> publish(message, parseProps(message)))
      .collect().asList()
      .replaceWithVoid()
      .subscribe()
      .with(
        avoid -> LOGGER.info("Messages published to " + queueClass.getName() + " entry ! "),
        throwable -> LOGGER.error("Unable to publish messages to " + queueClass.getName() + " -> " + new JsonArray(entries).encodePrettily(), throwable)
      );
  }

  public Uni<Void> enqueue(Message<T> message) {
    return publish(message, parseProps(message));
  }

  public void enqueueAndForget(Message<T> message) {
    publish(message, parseProps(message))
      .subscribe()
      .with(
        avoid -> LOGGER.info("Message published to " + queueClass.getName() + " entry -> " + message.id()),
        throwable -> LOGGER.error("Unable to publish message to " + queueClass.getName() + " -> " + JsonObject.mapFrom(message).encodePrettily(), throwable)
      );
  }


  private Uni<Void> publish(final Message<T> message, final AMQP.BasicProperties props) {
    return rabbitMQClient.basicPublish(
      "",
      queueClass.getName(),
      props,
      Buffer.newInstance(JsonObject.mapFrom(message).toBuffer())
    );
  }

  private Uni<Void> publishAndWaitForConfirmation(final Message<T> message, final AMQP.BasicProperties props) {
    return rabbitMQClient.confirmSelect()
      .flatMap(avoid -> rabbitMQClient.basicPublish(
          "",
          queueClass.getName(),
          props,
          Buffer.newInstance(JsonObject.mapFrom(message).toBuffer())
        )
      )
      .flatMap(avoid -> rabbitMQClient.waitForConfirms());
  }

  private AMQP.BasicProperties parseProps(final Message<T> message) {
    final var config = new AMQP.BasicProperties().builder().messageId(message.id());
    if (message.priority() != null) {
      config.priority(message.priority());
    }
    if (message.expiration() != null) {
      config.expiration(message.expiration().toString());
    }
    if (message.scheduled() != null) {
      final var xDelay = new HashMap<String, Object>();
      xDelay.put("x-delay", Duration.between(Instant.now(), message.scheduled()).toMillis());
      config.headers(xDelay);
    }
    return config.build();
  }

}
