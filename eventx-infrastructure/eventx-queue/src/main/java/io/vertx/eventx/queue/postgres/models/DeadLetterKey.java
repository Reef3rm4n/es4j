package io.vertx.eventx.queue.postgres.models;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.eventx.sql.models.RepositoryRecordKey;

@RecordBuilder
public record DeadLetterKey(
  String messageID,
  String tenant
) implements RepositoryRecordKey {
}
