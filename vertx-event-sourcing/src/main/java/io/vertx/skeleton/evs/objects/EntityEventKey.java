package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.sql.models.RepositoryRecordKey;

public record EntityEventKey(
  String entityId,
  Long eventVersion,
  String tenant
) implements RepositoryRecordKey {
}
