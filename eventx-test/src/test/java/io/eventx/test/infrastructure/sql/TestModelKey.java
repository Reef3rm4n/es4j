package io.eventx.test.infrastructure.sql;

import io.eventx.sql.models.RepositoryRecordKey;

public record TestModelKey(
  String textField
) implements RepositoryRecordKey {
}
