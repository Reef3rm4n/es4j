package io.es4j.infra.pg.models;

import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;

public record ProjectionOffset(
  String entityId,
  String projectionClass,
  Long lastAggregateVersion,
  BaseRecord baseRecord
) implements RepositoryRecord<ProjectionOffset> {

  @Override
  public ProjectionOffset with(BaseRecord baseRecord) {
    return new ProjectionOffset(
      entityId,
      projectionClass,
      lastAggregateVersion,
      baseRecord
    );
  }


  public ProjectionOffset incrementVersion(Long lastEventVersion) {
    return new ProjectionOffset(entityId, projectionClass, lastEventVersion,  baseRecord);
  }
}
