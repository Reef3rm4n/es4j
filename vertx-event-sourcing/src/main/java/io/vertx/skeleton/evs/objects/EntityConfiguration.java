package io.vertx.skeleton.evs.objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class EntityConfiguration {
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

  public EntityConfiguration setHandlerHeartBeatInterval(Integer handlerHeartBeatInterval) {
    this.handlerHeartBeatInterval = handlerHeartBeatInterval;
    return this;
  }

  public Boolean replication() {
    return replication;
  }

  public EntityConfiguration setReplication(final Boolean replication) {
    this.replication = replication;
    return this;
  }

  public PersistenceMode persistenceMode() {
    return persistenceMode;
  }

  public EntityConfiguration setPersistenceMode(final PersistenceMode persistenceMode) {
    this.persistenceMode = persistenceMode;
    return this;
  }

  public Long aggregateCacheTtlInMinutes() {
    return aggregateCacheTtlInMinutes;
  }

  public EntityConfiguration setAggregateCacheTtlInMinutes(final Long aggregateCacheTtlInMinutes) {
    this.aggregateCacheTtlInMinutes = aggregateCacheTtlInMinutes;
    return this;
  }

  public EntityConfiguration() {
  }

  public Boolean snapshots() {
    return snapshots;
  }

  public EntityConfiguration setSnapshots(final Boolean snapshots) {
    this.snapshots = snapshots;
    return this;
  }

  public Boolean useCache() {
    return useCache;
  }

  public EntityConfiguration setUseCache(final Boolean useCache) {
    this.useCache = useCache;
    return this;
  }

  public Integer snapshotEvery() {
    return snapshotEvery;
  }

  public EntityConfiguration setSnapshotEvery(final Integer snapshotEvery) {
    this.snapshotEvery = snapshotEvery;
    return this;
  }

  public Integer maxNumberOfCommandsForIdempotency() {
    return maxNumberOfCommandsForIdempotency;
  }

  public EntityConfiguration setMaxNumberOfCommandsForIdempotency(final Integer maxNumberOfCommandsForIdempotency) {
    this.maxNumberOfCommandsForIdempotency = maxNumberOfCommandsForIdempotency;
    return this;
  }
}
