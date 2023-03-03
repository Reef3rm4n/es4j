package io.vertx.eventx.infrastructure.pg.models;

import io.vertx.core.json.JsonObject;
import io.vertx.eventx.sql.models.RepositoryRecord;
import io.vertx.eventx.sql.models.BaseRecord;

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
