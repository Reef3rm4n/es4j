package io.vertx.eventx.test.domain;

import io.vertx.eventx.Aggregate;

import java.util.Map;

public record FakeAggregate(
  String entityId,
  Map<String, Object> data
) implements Aggregate {

  public FakeAggregate replaceData(Map<String, Object> newData) {
    return new FakeAggregate(
      entityId,
      newData
    );
  }

}
