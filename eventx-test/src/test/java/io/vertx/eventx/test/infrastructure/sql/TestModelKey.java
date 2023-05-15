package io.vertx.eventx.test.infrastructure.sql;

import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record TestModelKey(
  String textField
) implements RepositoryRecordKey {
}
