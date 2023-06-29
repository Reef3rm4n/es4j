package io.es4j.infra.pg.models;

import io.es4j.sql.models.RepositoryRecordKey;

public record EventRecordKey(
  Long id
) implements RepositoryRecordKey {

  @Override
  public String tenantId() {
    return "default";
  }
}
