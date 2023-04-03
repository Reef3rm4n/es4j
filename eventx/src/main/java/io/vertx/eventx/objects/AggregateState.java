package io.vertx.eventx.objects;


import com.google.common.collect.EvictingQueue;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Aggregate;

import io.vertx.core.shareddata.Shareable;

import java.util.List;

public class AggregateState<T extends Aggregate> implements Shareable {

  private final Class<T> aggregateClass;
  private T state = null;
  private final EvictingQueue<String> knownCommands = EvictingQueue.create(100);
  private Long currentVersion = null;

  private Long currentJournalOffset = null;

  public AggregateState(
    Class<T> aggregateClass
  ) {
    this.aggregateClass = aggregateClass;
  }


  public Class<T> aggregateClass() {
    return aggregateClass;
  }

  public Long currentVersion() {
    return currentVersion;
  }

  public AggregateState<T> setCurrentVersion(Long currentVersion) {
    this.currentVersion = currentVersion;
    return this;
  }

  public T state() {
    return state;
  }

  public AggregateState<T> setState(final T state) {
    this.state = state;
    return this;
  }

  public EvictingQueue<String> knownCommands() {
    return knownCommands;
  }

  public AggregateState<T> addKnownCommand(String commandId) {
    if (!knownCommands.contains(commandId)) {
      knownCommands.add(commandId);
    }
    return this;
  }
  public AggregateState<T> addKnownCommands(List<String> commandIds) {
    commandIds.stream()
      .filter(cmd -> !knownCommands.contains(cmd))
      .forEach(knownCommands::add);
    return this;
  }

  public JsonObject toJson() {
    return new JsonObject()
      .put("state", JsonObject.mapFrom(state))
      .put("currentVersion", currentVersion)
      .put("knownCommands", knownCommands.stream().toList());
  }

  public static <X extends Aggregate> AggregateState<X> fromJson(JsonObject jsonObject, Class<X> tClass) {
    return new AggregateState<>(
      tClass
    )
      .setCurrentVersion(jsonObject.getLong("currentVersion"))
      .addKnownCommands(jsonObject.getJsonArray("knownCommands").stream().map(String::valueOf).toList())
      .setState(jsonObject.getJsonObject("state").mapTo(tClass));
  }

  public Long currentJournalOffset() {
    return currentJournalOffset;
  }

  public AggregateState<T> setCurrentJournalOffset(Long currentJournalOffset) {
    this.currentJournalOffset = currentJournalOffset;
    return this;
  }
}
