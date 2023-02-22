package io.vertx.skeleton.taskqueue.postgres.models;

import io.vertx.skeleton.sql.models.RepositoryRecordKey;

public record MessageTransactionID(
  String messageId,
  String tenant
) implements RepositoryRecordKey {
}
