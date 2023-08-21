package io.es4j.infrastructure.pgbroker.models;


import io.es4j.infrastructure.pgbroker.ConsumerTransactionProvider;
import io.es4j.infrastructure.pgbroker.vertx.VertxConsumerTransaction;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Duration;

@RecordBuilder
public record BrokerConfiguration(
  Duration messageDurability,
  Duration consumerTxDurability,
  ConsumerTransactionProvider consumerTransactionProvider,
  Duration consumerThrottle,
  Integer consumerConcurrency,
  Duration messageMaxProcessingTime,
  Long brokerBatchingSize
) {


  public static BrokerConfiguration defaultConfiguration() {
    return BrokerConfigurationBuilder.builder()
      .messageDurability(Duration.ofDays(5))
      .consumerTxDurability(Duration.ofDays(5))
      .consumerTransactionProvider(new VertxConsumerTransaction())
      .consumerThrottle(Duration.ofMillis(10))
      .consumerConcurrency(100)
      .messageMaxProcessingTime(Duration.ofMinutes(30))
      .brokerBatchingSize(1000L)
      .build();
  }


}
