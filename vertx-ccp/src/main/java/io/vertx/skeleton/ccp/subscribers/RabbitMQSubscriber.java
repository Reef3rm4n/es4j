package io.vertx.skeleton.ccp.subscribers;

import io.vertx.skeleton.ccp.consumers.SingleProcessHandler;
import io.vertx.skeleton.ccp.models.MessageRecord;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;
import io.vertx.mutiny.rabbitmq.RabbitMQMessage;
import io.vertx.rabbitmq.QueueOptions;

public class RabbitMQSubscriber<T, R> {
  private final RabbitMQClient rabbitMQClient;
  private final SingleProcessHandler<T, R> taskWrapper;

  public RabbitMQSubscriber(final RabbitMQClient rabbitMQClient, final SingleProcessHandler<T, R> singleProcessHandler) {
    this.rabbitMQClient = rabbitMQClient;
    this.taskWrapper = singleProcessHandler;
  }

  public void start() {
    final var queueOptions = new QueueOptions().setAutoAck(false);
    rabbitMQClient.getDelegate().addConnectionEstablishedCallback(
      promise -> {
        final var config = new JsonObject().put("x-max-priority", 10);
        Uni.join().all(
            rabbitMQClient.queueDeclare(
                taskWrapper.configuration().queueName(),
                true,
                false,
                false,
                config
              )
              .replaceWithVoid()

          )
          .andFailFast()
          .subscribe()
          .with(item -> promise.complete(), promise::fail);
      }
    );
    rabbitMQClient.start().flatMap(avoid ->
        rabbitMQClient.basicConsumer(taskWrapper.configuration().queueName(), queueOptions)
          .map(rabbitMQConsumer -> rabbitMQConsumer
            .handler(this::handleQueueEntry)
            .exceptionHandler(this::handleException)
          )

      )
      .subscribe()
      .with(
        item -> taskWrapper.logger().info("Consumer registered to queue" + item.queueName()),
        throwable -> taskWrapper.logger().error("Unable to bootstrap queue", throwable)
      );
  }

  private void handleException(final Throwable throwable) {
    taskWrapper.logger().error("[-- RabbitMQJobQueue had to drop the following exception --]", throwable);
  }

  private void handleQueueEntry(final RabbitMQMessage rabbitMQMessage) {
    final var entry = new JsonObject(rabbitMQMessage.body().getDelegate()).mapTo(MessageRecord.class);
//    taskWrapper.process(List.of(entry))
//      .flatMap(result -> Multi.createFrom().iterable(result)
//        .onItem().transformToUniAndMerge(
//          qe -> {
//            if (qe.taskState() == RETRY) {
//              return rabbitMQClient.basicPublish("", taskWrapper.configuration().queueName(), Buffer.newInstance(JsonObject.mapFrom(qe).toBuffer()))
//                .flatMap(av -> rabbitMQClient.basicAck(rabbitMQMessage.getDelegate().envelope().getDeliveryTag(), false));
//            } else {
//              return rabbitMQClient.basicAck(rabbitMQMessage.getDelegate().envelope().getDeliveryTag(), false);
//            }
//          }
//        ).collect().asList()
//        .replaceWithVoid())
//      .subscribe()
//      .with(
//        avoid -> taskWrapper.logger().info("Message handled -> " + rabbitMQMessage.properties().getMessageId()),
//        throwable -> taskWrapper.logger().error("Unable to handle message", throwable)
//      );
  }


  public Uni<Void> stop() {
    return rabbitMQClient.stop();
  }
}
