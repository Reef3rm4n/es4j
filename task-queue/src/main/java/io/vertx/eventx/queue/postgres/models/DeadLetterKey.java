package io.vertx.eventx.queue.postgres.models;

import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record DeadLetterKey(
  String messageID,
  String tenant
) implements RepositoryRecordKey {
}
