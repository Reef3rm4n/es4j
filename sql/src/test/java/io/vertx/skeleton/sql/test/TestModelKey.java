package io.vertx.skeleton.sql.test;

import io.vertx.skeleton.sql.models.RepositoryRecordKey;

public record TestModelKey(
  String textField
) implements RepositoryRecordKey {
}
