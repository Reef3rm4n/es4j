package io.vertx.skeleton.evs.objects;

import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.sql.models.RepositoryRecord;
import io.vertx.skeleton.sql.models.BaseRecord;

public record RejectedCommand(
  String entityId,
  JsonObject command,
  String commandClass,
  JsonObject error,
  BaseRecord baseRecord
) implements RepositoryRecord<RejectedCommand> {
  @Override
  public RejectedCommand with(final BaseRecord baseRecord) {
    return new RejectedCommand(entityId, command, commandClass, error, baseRecord);
  }
}
