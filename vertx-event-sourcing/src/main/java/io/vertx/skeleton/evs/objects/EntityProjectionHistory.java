package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.sql.models.BaseRecord;
import io.vertx.skeleton.sql.models.RepositoryRecord;

public record EntityProjectionHistory(
  String entityId,
  String projectionClass,
  Long lastEventVersion,
  BaseRecord baseRecord
) implements RepositoryRecord<EntityProjectionHistory> {

  @Override
  public EntityProjectionHistory with(BaseRecord baseRecord) {
    return new EntityProjectionHistory(
      entityId,
      projectionClass,
      lastEventVersion,
      baseRecord
    );
  }


  public EntityProjectionHistory incrementVersion(Long lastEventVersion) {
    return new EntityProjectionHistory(entityId, projectionClass, lastEventVersion, baseRecord);
  }
}
