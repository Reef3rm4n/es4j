package io.eventx.queue.postgres.models;

import io.eventx.sql.models.RepositoryRecordKey;
import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record MessageRecordID(
  String id,
  String tenant
) implements RepositoryRecordKey {
}
