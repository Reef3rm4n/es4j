package io.vertx.skeleton.evs.objects;

import io.vertx.core.shareddata.Shareable;
import io.vertx.skeleton.sql.models.RepositoryRecordKey;

public record EntityKey(
  String entityId,
  String tenant
) implements RepositoryRecordKey, Shareable {
}
