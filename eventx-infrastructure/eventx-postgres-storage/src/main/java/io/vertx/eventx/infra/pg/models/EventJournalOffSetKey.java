package io.vertx.eventx.infra.pg.models;


import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record EventJournalOffSetKey(
  String consumer,
  String tenantId
) implements RepositoryRecordKey {
}
