package io.vertx.skeleton.evs.objects;

import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.models.PersistedRecord;
import io.vertx.skeleton.models.RepositoryRecord;

public record RejectedCommand(
  String entityId,
  JsonObject command,
  String commandClass,
  JsonObject error,
  PersistedRecord persistedRecord
) implements RepositoryRecord<RejectedCommand> {
  @Override
  public RejectedCommand with(final PersistedRecord persistedRecord) {
    return new RejectedCommand(entityId, command, commandClass, error, persistedRecord);
  }
}
