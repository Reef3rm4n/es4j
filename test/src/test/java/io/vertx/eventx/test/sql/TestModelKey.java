package io.vertx.eventx.test.sql;

import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record TestModelKey(
  String textField
) implements RepositoryRecordKey {
}
