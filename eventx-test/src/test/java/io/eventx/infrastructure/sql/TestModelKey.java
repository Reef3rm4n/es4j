package io.eventx.infrastructure.sql;

import io.eventx.sql.models.RepositoryRecordKey;

public record TestModelKey(
  String textField
) implements RepositoryRecordKey {
}
