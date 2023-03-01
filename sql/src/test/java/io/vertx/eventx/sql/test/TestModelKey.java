package io.vertx.eventx.sql.test;

import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record TestModelKey(
  String textField
) implements RepositoryRecordKey {
}
