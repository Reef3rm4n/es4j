package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.sql.models.RepositoryRecordKey;

public record MessageTransactionID(
  String messageId,
  String processorClass,
  String tenant
) implements RepositoryRecordKey {
}
