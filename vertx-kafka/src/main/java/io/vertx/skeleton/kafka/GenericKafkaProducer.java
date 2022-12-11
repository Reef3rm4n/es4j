package io.vertx.skeleton.kafka;

import io.vertx.skeleton.models.KeyValue;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.producer.KafkaProducer;
import io.vertx.mutiny.kafka.client.producer.KafkaProducerRecord;

import java.util.List;
import java.util.Map;

public class GenericKafkaProducer<V extends KeyValue<?, V>> {

  private final String topic;
  private final KafkaProducer<JsonObject, JsonObject> producer;
  private static final Logger logger = LoggerFactory.getLogger(GenericKafkaProducer.class);
  private final Class<V> vClass;

  public GenericKafkaProducer(String server, String topic, String groupId, Vertx vertx, Class<V> vClass) {
    this.vClass = vClass;
    this.topic = topic;
    this.producer = io.vertx.mutiny.kafka.client.producer.KafkaProducer.createShared(
      vertx,
      groupId,
      new KafkaClientOptions()
        .setConfig(
          Map.of(
            "bootstrap.servers", server,
            "key.serializer", "io.vertx.kafka.client.serialization.JsonObjectSerializer",
            "value.serializer", "io.vertx.kafka.client.serialization.JsonObjectSerializer",
            "linger.ms", 2000
          )
        )
    );
    producer.exceptionHandler(this::handleException);
  }

  private void handleException(Throwable throwable) {
    logger.error("Error in producer " + vClass + " for topic :" + topic, throwable);
  }

  public Uni<Void> write(V entity) {
    final var record = KafkaProducerRecord.create(topic, JsonObject.mapFrom(entity.getKey()), JsonObject.mapFrom(entity.getValue()));
    return producer.send(record).map(result -> {
        logger.debug("key=" + record.key() + ",value=" + record.value() + ",partition=" + record.partition() + ",topic=" + record.topic());
        return result;
      }
    ).onFailure().invoke(
      throwable -> logger.error("Unable to send record " + record + " to topic :" + topic, throwable)
    ).replaceWithVoid();
  }

  public Uni<Void> writeBatch(List<V> batch) {
    return Multi.createFrom().iterable(batch)
      .onItem().transformToUniAndMerge(entity -> write(entity.getValue()))
      .collect().asList()
      .replaceWithVoid();
  }


}
