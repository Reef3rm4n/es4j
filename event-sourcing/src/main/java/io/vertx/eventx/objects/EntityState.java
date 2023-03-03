package io.vertx.eventx.objects;


import com.google.common.collect.EvictingQueue;
import io.vertx.eventx.Aggregate;

import io.vertx.core.shareddata.Shareable;
import io.vertx.eventx.infrastructure.models.Event;
import io.vertx.eventx.infrastructure.pg.models.AggregateSnapshotRecord;
import io.vertx.eventx.infrastructure.pg.models.EventRecord;


import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class EntityState<T extends Aggregate> implements Shareable {

  private T aggregateState = null;
  private final List<Event> eventsAfterSnapshot;
  private final EvictingQueue<String> commands;
  private Integer processedEventsAfterLastSnapshot = 0;
  private  Integer snapshotAfter;
  private Boolean snapshotPresent = false;
  private AggregateSnapshotRecord snapshot = null;
  private Long currentEventVersion = null;

  private Long provisoryEventVersion = null;

  public EntityState(final Integer snapshotAfter, final Integer numberOfCommandsKeptForIdempotency) {
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

  public EntityState<T> setAggregateState(final T aggregateState) {
    this.aggregateState = aggregateState;
    return this;
  }

  public EvictingQueue<String> commands() {
    return commands;
  }

  public Long provisoryEventVersion() {
    return provisoryEventVersion;
  }

  public EntityState<T> setProvisoryEventVersion(final Long provisoryEventVersion) {
    this.provisoryEventVersion = provisoryEventVersion;
    return this;
  }

  public List<Event> eventsAfterSnapshot() {
    return eventsAfterSnapshot;
  }

  public Integer processedEventsAfterLastSnapshot() {
    return processedEventsAfterLastSnapshot;
  }

  public EntityState<T> setProcessedEventsAfterLastSnapshot(final Integer processedEventsAfterLastSnapshot) {
    this.processedEventsAfterLastSnapshot = processedEventsAfterLastSnapshot;
    return this;
  }

  public Integer snapshotAfter() {
    return snapshotAfter;
  }

  public EntityState<T> setSnapshotAfter(final Integer snapshotAfter) {
    this.snapshotAfter = snapshotAfter;
    return this;
  }

  public Boolean snapshotPresent() {
    return snapshotPresent;
  }

  public EntityState<T> setSnapshotPresent(final Boolean snapshotPresent) {
    this.snapshotPresent = snapshotPresent;
    return this;
  }

  public AggregateSnapshotRecord snapshot() {
    return snapshot;
  }

  public EntityState<T> setSnapshot(final AggregateSnapshotRecord snapshot) {
    this.snapshot = snapshot;
    return this;
  }

  public Long currentEventVersion() {
    return currentEventVersion;
  }

  public EntityState<T> setCurrentEventVersion(final Long currentEventVersion) {
    this.currentEventVersion = currentEventVersion;
    return this;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", EntityState.class.getSimpleName() + "[", "]")
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
