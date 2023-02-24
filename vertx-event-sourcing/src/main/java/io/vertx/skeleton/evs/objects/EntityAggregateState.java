package io.vertx.skeleton.evs.objects;


import com.google.common.collect.EvictingQueue;
import io.vertx.skeleton.evs.Entity;

import io.vertx.core.shareddata.Shareable;


import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class EntityAggregateState<T extends Entity> implements Shareable {

  private T aggregateState = null;
  private final List<EntityEvent> eventsAfterSnapshot;
  private final EvictingQueue<String> commands;
  private Integer processedEventsAfterLastSnapshot = 0;
  private  Integer snapshotAfter;
  private Boolean snapshotPresent = false;
  private AggregateSnapshot snapshot = null;
  private Long currentEventVersion = null;

  private Long provisoryEventVersion = null;

  public EntityAggregateState(final Integer snapshotAfter, final Integer numberOfCommandsKeptForIdempotency) {
    this.snapshotAfter = snapshotAfter;
    this.eventsAfterSnapshot = new ArrayList<>(snapshotAfter + 1);
    if (numberOfCommandsKeptForIdempotency > 0) {
      this.commands = EvictingQueue.create(numberOfCommandsKeptForIdempotency);
    } else {
      this.commands = null;
    }
  }

  public T aggregateState() {
    return aggregateState;
  }

  public EntityAggregateState<T> setAggregateState(final T aggregateState) {
    this.aggregateState = aggregateState;
    return this;
  }

  public EvictingQueue<String> commands() {
    return commands;
  }

  public Long provisoryEventVersion() {
    return provisoryEventVersion;
  }

  public EntityAggregateState<T> setProvisoryEventVersion(final Long provisoryEventVersion) {
    this.provisoryEventVersion = provisoryEventVersion;
    return this;
  }

  public List<EntityEvent> eventsAfterSnapshot() {
    return eventsAfterSnapshot;
  }

  public Integer processedEventsAfterLastSnapshot() {
    return processedEventsAfterLastSnapshot;
  }

  public EntityAggregateState<T> setProcessedEventsAfterLastSnapshot(final Integer processedEventsAfterLastSnapshot) {
    this.processedEventsAfterLastSnapshot = processedEventsAfterLastSnapshot;
    return this;
  }

  public Integer snapshotAfter() {
    return snapshotAfter;
  }

  public EntityAggregateState<T> setSnapshotAfter(final Integer snapshotAfter) {
    this.snapshotAfter = snapshotAfter;
    return this;
  }

  public Boolean snapshotPresent() {
    return snapshotPresent;
  }

  public EntityAggregateState<T> setSnapshotPresent(final Boolean snapshotPresent) {
    this.snapshotPresent = snapshotPresent;
    return this;
  }

  public AggregateSnapshot snapshot() {
    return snapshot;
  }

  public EntityAggregateState<T> setSnapshot(final AggregateSnapshot snapshot) {
    this.snapshot = snapshot;
    return this;
  }

  public Long currentEventVersion() {
    return currentEventVersion;
  }

  public EntityAggregateState<T> setCurrentEventVersion(final Long currentEventVersion) {
    this.currentEventVersion = currentEventVersion;
    return this;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", EntityAggregateState.class.getSimpleName() + "[", "]")
      .add("aggregateState=" + aggregateState)
      .add("eventsAfterSnapshot=" + eventsAfterSnapshot)
      .add("commands=" + commands)
      .add("processedEventsAfterLastSnapshot=" + processedEventsAfterLastSnapshot)
      .add("snapshotAfter=" + snapshotAfter)
      .add("snapshotPresent=" + snapshotPresent)
      .add("snapshot=" + snapshot)
      .add("currentEventVersion=" + currentEventVersion)
      .add("provisoryEventVersion=" + provisoryEventVersion)
      .toString();
  }
}
