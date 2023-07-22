package io.es4j.infrastructure.pgbroker.models;


import io.es4j.infrastructure.pgbroker.ConsumerTransactionProvider;
import io.es4j.infrastructure.pgbroker.vertx.VertxConsumerTransaction;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Duration;

@RecordBuilder
public record PgBrokerConfiguration(
  Boolean bootstrapQueue,
  Boolean idempotency,
  Duration purgeTxAfter,
  ConsumerTransactionProvider consumerTransactionProvider,
  Duration emptyBackOff,
  Duration throttle,
  Integer concurrency,
  Long errorBackOffInMinutes,
  Duration retryBackOffInterval,
  Duration maxProcessingTime,
  Long batchSize,
  Integer maxRetry,
  Duration maintenanceEvery
) {


  public static PgBrokerConfiguration defaultConfiguration() {
    return PgBrokerConfigurationBuilder.builder()
      .bootstrapQueue(false)
      .idempotency(true)
      .concurrency(100)
      .purgeTxAfter(Duration.ofDays(5))
      .retryBackOffInterval(Duration.ofMillis(10))
      .maxProcessingTime(Duration.ofMinutes(30))
      .consumerTransactionProvider(new VertxConsumerTransaction())
      .batchSize(10L)
      .maxRetry(5)
      .throttle(Duration.ofMillis(10))
      .maintenanceEvery(Duration.ofMinutes(30))
      .build();
  }


}
