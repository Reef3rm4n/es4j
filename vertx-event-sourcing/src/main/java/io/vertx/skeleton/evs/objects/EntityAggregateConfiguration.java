package io.vertx.skeleton.evs.objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class EntityAggregateConfiguration {
  private Boolean useCache = true;
  private Long aggregateCacheTtlInMinutes = 20L;
  private Boolean snapshots = true;
  private Integer snapshotEvery = 10;
  private Integer maxNumberOfCommandsForIdempotency = 50;
  private PersistenceMode persistenceMode = PersistenceMode.DATABASE;
  private Boolean replication = false;
  private Integer handlerHeartBeatInterval = 1000;

  public Integer handlerHeartBeatInterval() {
    return handlerHeartBeatInterval;
  }

  public EntityAggregateConfiguration setHandlerHeartBeatInterval(Integer handlerHeartBeatInterval) {
    this.handlerHeartBeatInterval = handlerHeartBeatInterval;
    return this;
  }

  public Boolean replication() {
    return replication;
  }

  public EntityAggregateConfiguration setReplication(final Boolean replication) {
    this.replication = replication;
    return this;
  }

  public PersistenceMode persistenceMode() {
    return persistenceMode;
  }

  public EntityAggregateConfiguration setPersistenceMode(final PersistenceMode persistenceMode) {
    this.persistenceMode = persistenceMode;
    return this;
  }

  public Long aggregateCacheTtlInMinutes() {
    return aggregateCacheTtlInMinutes;
  }

  public EntityAggregateConfiguration setAggregateCacheTtlInMinutes(final Long aggregateCacheTtlInMinutes) {
    this.aggregateCacheTtlInMinutes = aggregateCacheTtlInMinutes;
    return this;
  }

  public EntityAggregateConfiguration() {
  }

  public Boolean snapshots() {
    return snapshots;
  }

  public EntityAggregateConfiguration setSnapshots(final Boolean snapshots) {
    this.snapshots = snapshots;
    return this;
  }

  public Boolean useCache() {
    return useCache;
  }

  public EntityAggregateConfiguration setUseCache(final Boolean useCache) {
    this.useCache = useCache;
    return this;
  }

  public Integer snapshotEvery() {
    return snapshotEvery;
  }

  public EntityAggregateConfiguration setSnapshotEvery(final Integer snapshotEvery) {
    this.snapshotEvery = snapshotEvery;
    return this;
  }

  public Integer maxNumberOfCommandsForIdempotency() {
    return maxNumberOfCommandsForIdempotency;
  }

  public EntityAggregateConfiguration setMaxNumberOfCommandsForIdempotency(final Integer maxNumberOfCommandsForIdempotency) {
    this.maxNumberOfCommandsForIdempotency = maxNumberOfCommandsForIdempotency;
    return this;
  }
}
