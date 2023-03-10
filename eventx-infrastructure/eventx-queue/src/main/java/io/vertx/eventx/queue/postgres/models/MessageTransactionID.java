package io.vertx.eventx.queue.postgres.models;

import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record MessageTransactionID(
  String messageId,
  String tenant
) implements RepositoryRecordKey {
}
