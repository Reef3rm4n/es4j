package io.vertx.eventx.infrastructure.pg.models;


import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record EventJournalOffSetKey(
  String consumer
) implements RepositoryRecordKey {
}
