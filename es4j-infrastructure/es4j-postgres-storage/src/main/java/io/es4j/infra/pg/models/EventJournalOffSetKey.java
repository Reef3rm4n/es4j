package io.es4j.infra.pg.models;


import io.es4j.sql.models.RepositoryRecordKey;

public record EventJournalOffSetKey(
  String consumer,
  String tenantId
) implements RepositoryRecordKey {
}
