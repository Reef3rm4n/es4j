package io.eventx.infra.pg.models;

import io.eventx.sql.models.RepositoryRecordKey;

public record EventRecordKey(
  Long id
) implements RepositoryRecordKey {

  @Override
  public String tenantId() {
    return null;
  }
}
