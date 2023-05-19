package io.vertx.eventx.core.objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class AggregateConfiguration {
  private Boolean useCache = true;
  private Long aggregateCacheTtlInMinutes = 20L;
  private Boolean snapshots = true;
  private Integer snapshotEvery = 50;
  private Integer maxNumberOfCommandsForIdempotency = 50;
  private Boolean replication = false;


  public Boolean replication() {
    return replication;
  }

  public AggregateConfiguration setReplication(final Boolean replication) {
    this.replication = replication;
    return this;
  }

  public Long aggregateCacheTtlInMinutes() {
    return aggregateCacheTtlInMinutes;
  }

  public AggregateConfiguration setAggregateCacheTtlInMinutes(final Long aggregateCacheTtlInMinutes) {
    this.aggregateCacheTtlInMinutes = aggregateCacheTtlInMinutes;
    return this;
  }

  public AggregateConfiguration() {
  }

  public Boolean snapshots() {
    return snapshots;
  }

  public AggregateConfiguration setSnapshots(final Boolean snapshots) {
    this.snapshots = snapshots;
    return this;
  }

  public Boolean useCache() {
    return useCache;
  }

  public AggregateConfiguration setUseCache(final Boolean useCache) {
    this.useCache = useCache;
    return this;
  }

  public Integer snapshotEvery() {
    return snapshotEvery;
  }

  public AggregateConfiguration setSnapshotEvery(final Integer snapshotEvery) {
    this.snapshotEvery = snapshotEvery;
    return this;
  }

  public Integer maxNumberOfCommandsForIdempotency() {
    return maxNumberOfCommandsForIdempotency;
  }

  public AggregateConfiguration setMaxNumberOfCommandsForIdempotency(final Integer maxNumberOfCommandsForIdempotency) {
    this.maxNumberOfCommandsForIdempotency = maxNumberOfCommandsForIdempotency;
    return this;
  }
}
