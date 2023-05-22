package io.eventx.queue.postgres.models;

import io.eventx.sql.models.RepositoryRecordKey;
import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record MessageTransactionID(
  String messageId,
  String tenant
) implements RepositoryRecordKey {
}
