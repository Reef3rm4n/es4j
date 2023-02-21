package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.sql.models.RepositoryRecordKey;

public record MessageRecordID(
  String id,
  String tenant
) implements RepositoryRecordKey {
}
