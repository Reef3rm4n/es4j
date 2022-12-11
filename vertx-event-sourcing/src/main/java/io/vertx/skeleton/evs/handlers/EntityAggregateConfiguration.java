package io.vertx.skeleton.evs.handlers;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class EntityAggregateConfiguration {
  private Boolean useCache = true;
  private Long aggregateCacheTtlInMinutes = 20L;
  private String eventJournalTable;
  private String snapshotTable;
  private String rejectCommandTable;
  private Boolean snapshots = true;
  private Integer snapshotEvery = 20;

  private Integer maxNumberOfCommandsForIdempotency = 20;

  private PersistenceMode persistenceMode = PersistenceMode.DATABASE;
  private Boolean replication = false;

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


  public String eventJournalTable() {
    return eventJournalTable;
  }

  public EntityAggregateConfiguration setEventJournalTable(final String eventJournalTable) {
    this.eventJournalTable = eventJournalTable;
    return this;
  }

  public String snapshotTable() {
    return snapshotTable;
  }

  public EntityAggregateConfiguration setSnapshotTable(final String snapshotTable) {
    this.snapshotTable = snapshotTable;
    return this;
  }

  public String rejectCommandTable() {
    return rejectCommandTable;
  }

  public EntityAggregateConfiguration setRejectCommandTable(final String rejectCommandTable) {
    this.rejectCommandTable = rejectCommandTable;
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
