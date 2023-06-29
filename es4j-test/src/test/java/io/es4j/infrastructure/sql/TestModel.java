package io.es4j.infrastructure.sql;

import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;
import io.vertx.core.json.JsonObject;

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
