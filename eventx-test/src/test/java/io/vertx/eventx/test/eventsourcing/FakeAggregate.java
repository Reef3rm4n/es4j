package io.vertx.eventx.test.eventsourcing;

import io.vertx.eventx.Aggregate;

import java.util.Map;

public record FakeAggregate(
  String aggregateId,
  Map<String, Object> data
) implements Aggregate {

  public FakeAggregate replaceData(Map<String, Object> newData) {
    return new FakeAggregate(
      aggregateId,
      newData
    );
  }

}