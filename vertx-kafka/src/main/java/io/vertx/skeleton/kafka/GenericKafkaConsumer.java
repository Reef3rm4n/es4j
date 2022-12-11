package io.vertx.skeleton.kafka;

import io.vertx.skeleton.models.KeyValue;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecords;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class GenericKafkaConsumer<V extends KeyValue<?, V>> {

  private final KafkaConsumer<JsonObject, JsonObject> kafkaConsumer;
  private final String topic;
  private final Class<V> vClass;

  private static final Logger logger = LoggerFactory.getLogger(GenericKafkaConsumer.class);
  private final Vertx vertx;

  public GenericKafkaConsumer(String topic, String server, String groupId, Vertx vertx, Class<V> vClass) {
    this.topic = topic;
    this.vClass = vClass;
    this.vertx = vertx;
    this.kafkaConsumer = KafkaConsumer.create(vertx, new KafkaClientOptions()
      .setConfig(
        Map.of(
          "bootstrap.servers", server,
          "key.deserializer", "io.vertx.kafka.client.serialization.JsonObjectDeserializer",
          "value.deserializer", "io.vertx.kafka.client.serialization.JsonObjectDeserializer",
          "group.id", groupId,
          "auto.offset.reset", "earliest",
          "enable.auto.commit", "true",
          "max.poll.records", 1000
        )
      )
    );
    kafkaConsumer.exceptionHandler(this::handleException);
  }

  private void handleException(Throwable throwable) {
    logger.error("Error in consumer " + vClass + " for topic :" + topic, throwable);
  }

  public void subscribe(Consumer<V> consumer) {
    kafkaConsumer
      .handler(record -> consumer.accept(record.value().mapTo(vClass)))
      .subscribe(topic)
      .subscribe()
      .with(
        subscription -> logger.info("Subscribed consumer of " + vClass + " to topic :" + topic),
        throwable -> logger.error("Error subscribing consumer of " + vClass + " to topic :" + topic, throwable)
      );
  }

  public void poll(Function<List<V>, Uni<Void>> consumer) {
    vertx.setPeriodic(1000,
      timerId -> kafkaConsumer
        .poll(Duration.ofMillis(500))
        .flatMap(records -> consumer.apply(transformToList(records)))
        .subscribe()
        .with(
          subscription -> logger.info("Subscribed consumer of " + vClass + " to topic :" + topic),
          throwable -> logger.error("Error subscribing consumer of " + vClass + " to topic :" + topic, throwable)
        )
    );
  }

  private List<V> transformToList(KafkaConsumerRecords<JsonObject, JsonObject> records) {
    final var list = new ArrayList<V>();
    for (int i = 0; i < records.size(); i++) {
      KafkaConsumerRecord<JsonObject, JsonObject> record = records.recordAt(i);
      logger.debug("key=" + record.key() + ",value=" + record.value() +
        ",partition=" + record.partition() + ",offset=" + record.offset());
      list.add(record.value().mapTo(vClass));
    }
    return list;
  }

}
