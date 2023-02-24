package io.vertx.skeleton.evs.domain;

import io.vertx.skeleton.evs.Entity;

public record FakeEntity(
  String entityId,
  String data1,
  String data2,
  String data3,
  String data4,
  String data5
) implements Entity {

  public FakeEntity changeData1(String newData1) {
    return new FakeEntity(
      entityId,
      data1,
      data2,
      data3,
      data4,
      data5
    );
  }

  public FakeEntity create(String entityId) {
    return new FakeEntity(
      entityId,
      "data1",
      "data2",
      "data3",
      "data4",
      "data5"
    );
  }
}
