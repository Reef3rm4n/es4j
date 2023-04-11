package io.vertx.eventx.queue.postgres.models;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.eventx.sql.models.RepositoryRecordKey;

@RecordBuilder
public record MessageRecordID(
  String id,
  String tenant
) implements RepositoryRecordKey {
}
