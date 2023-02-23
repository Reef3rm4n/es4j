package io.vertx.skeleton.evs.objects;


import io.vertx.skeleton.sql.models.RepositoryRecordKey;

public record EventJournalOffSetKey(
  String consumer
) implements RepositoryRecordKey {
}
