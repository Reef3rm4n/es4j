package io.vertx.skeleton.sql.test;

import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.sql.models.BaseRecord;
import io.vertx.skeleton.sql.models.RepositoryRecord;

import java.time.Instant;


public record TestModel(
  String textField,
  Instant timeStampField,
  JsonObject jsonObjectField,
  Long longField,
  Integer integerField,
  BaseRecord baseRecord
) implements RepositoryRecord<TestModel> {
  @Override
  public TestModel with(BaseRecord baseRecord) {
    return new TestModel(textField, timeStampField, jsonObjectField, longField, integerField, baseRecord);
  }
}
