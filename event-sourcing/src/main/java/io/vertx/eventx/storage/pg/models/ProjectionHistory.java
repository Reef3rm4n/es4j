package io.vertx.eventx.storage.pg.models;

import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.RepositoryRecord;

public record ProjectionHistory(
  String entityId,
  String projectionClass,
  Long lastEventVersion,
  BaseRecord baseRecord
) implements RepositoryRecord<ProjectionHistory> {

  @Override
  public ProjectionHistory with(BaseRecord baseRecord) {
    return new ProjectionHistory(
      entityId,
      projectionClass,
      lastEventVersion,
      baseRecord
    );
  }


  public ProjectionHistory incrementVersion(Long lastEventVersion) {
    return new ProjectionHistory(entityId, projectionClass, lastEventVersion, baseRecord);
  }
}
