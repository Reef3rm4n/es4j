package io.vertx.skeleton.taskqueue.postgres.models;

import io.vertx.skeleton.sql.models.RepositoryRecordKey;

public record MessageRecordID(
  String id,
  String tenant
) implements RepositoryRecordKey {
}
