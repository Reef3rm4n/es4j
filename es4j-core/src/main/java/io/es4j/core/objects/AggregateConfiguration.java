package io.es4j.core.objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.time.Duration;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class AggregateConfiguration {
  private Duration cacheTtl = Duration.ofMinutes(40);
  private Integer snapshotThreshold = 500;
  private Integer idempotencyThreshold = 50;

  public Duration aggregateCacheTtlInMinutes() {
    return cacheTtl;
  }

  public AggregateConfiguration setCacheTtl(final Duration cacheTtl) {
    this.cacheTtl = cacheTtl;
    return this;
  }

  public Integer snapshotThreshold() {
    return snapshotThreshold;
  }

  public AggregateConfiguration setSnapshotThreshold(final Integer snapshotThreshold) {
    this.snapshotThreshold = snapshotThreshold;
    return this;
  }

  public Integer idempotencyThreshold() {
    return idempotencyThreshold;
  }

  public AggregateConfiguration setIdempotencyThreshold(final Integer idempotencyThreshold) {
    this.idempotencyThreshold = idempotencyThreshold;
    return this;
  }
}
