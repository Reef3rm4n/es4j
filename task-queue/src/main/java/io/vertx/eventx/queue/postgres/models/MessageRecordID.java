package io.vertx.eventx.queue.postgres.models;

import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record MessageRecordID(
  String id,
  String tenant
) implements RepositoryRecordKey {
}
