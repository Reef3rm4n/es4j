package io.vertx.skeleton.taskqueue.postgres.models;

import io.vertx.skeleton.sql.models.RepositoryRecordKey;

public record DeadLetterKey(
  String messageID,
  String tenant
) implements RepositoryRecordKey {
}
