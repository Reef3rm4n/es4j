package io.vertx.skeleton.sql.models;

import io.vertx.skeleton.sql.models.RepositoryRecordKey;

public record TestModelKey(
  String textField
) implements RepositoryRecordKey {
}
