package io.vertx.skeleton.evs.objects;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.models.PersistedRecord;
import io.vertx.skeleton.models.RepositoryRecord;

public record ConsumerFailure(
  String consumer,
  String entityId,
  Integer attempts,
  JsonArray errors,
  PersistedRecord persistedRecord
) implements RepositoryRecord<ConsumerFailure> {


  @Override
  public ConsumerFailure with(final PersistedRecord persistedRecord) {
    return new ConsumerFailure(consumer, entityId, attempts, errors, persistedRecord);
  }

  public ConsumerFailure withAttempts(final int i, JsonObject error) {
    return new ConsumerFailure(consumer, entityId, i, errors.add(error), persistedRecord);
  }
}
