package io.vertx.skeleton.sql.models;

import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.time.OffsetDateTime;

public record TestModel(
  String textField,
  OffsetDateTime timeStampField,
  JsonObject jsonObjectField,
  Long longField,
  Integer integerField,
  RecordWithoutID baseRecord
) implements RepositoryRecord<TestModel> {
  @Override
  public TestModel with(RecordWithoutID baseRecord) {
    return new TestModel(textField, timeStampField, jsonObjectField, longField, integerField, baseRecord);
  }
}
