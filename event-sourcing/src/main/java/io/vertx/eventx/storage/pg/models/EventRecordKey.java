package io.vertx.eventx.storage.pg.models;

import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record EventRecordKey(
  String entityId,
  Long eventVersion,
  String tenant
) implements RepositoryRecordKey {
}
