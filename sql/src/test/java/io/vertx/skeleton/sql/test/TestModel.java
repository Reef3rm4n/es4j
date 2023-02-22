package io.vertx.skeleton.sql.test;

import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.sql.models.RecordWithoutID;
import io.vertx.skeleton.sql.models.RepositoryRecord;

import java.time.Instant;


public record TestModel(
  String textField,
  Instant timeStampField,
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
