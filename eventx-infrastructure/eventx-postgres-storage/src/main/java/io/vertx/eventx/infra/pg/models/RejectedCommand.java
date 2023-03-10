package io.vertx.eventx.infra.pg.models;

import io.vertx.core.json.JsonObject;
import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.RepositoryRecord;

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
