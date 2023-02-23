package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.sql.models.RepositoryRecordKey;

public record EntityProjectionHistoryKey(
  String entityId,
  String projectionClass,
  String tenant
) implements RepositoryRecordKey {

}
