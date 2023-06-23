package io.eventx.domain;

import com.google.auto.service.AutoService;
import io.eventx.Aggregate;
import io.eventx.Bootstrap;

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
