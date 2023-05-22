package io.eventx.infra.pg.models;


import io.eventx.sql.models.RepositoryRecordKey;

public record EventJournalOffSetKey(
  String consumer,
  String tenantId
) implements RepositoryRecordKey {
}
