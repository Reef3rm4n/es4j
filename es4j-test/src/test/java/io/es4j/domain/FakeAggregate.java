package io.es4j.domain;

import io.es4j.Aggregate;

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
