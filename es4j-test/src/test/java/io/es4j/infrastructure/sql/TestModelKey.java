package io.es4j.infrastructure.sql;

import io.es4j.sql.models.RepositoryRecordKey;

public record TestModelKey(
  String textField
) implements RepositoryRecordKey {
}
