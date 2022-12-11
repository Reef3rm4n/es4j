package io.vertx.skeleton.models;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public record ConsumerFailure(
  String consumerName,
  Integer attempts,
  JsonObject event,
  JsonArray errors,
  PersistedRecord persistedRecord
) implements RepositoryRecord<ConsumerFailure> {


  @Override
  public ConsumerFailure with(final PersistedRecord persistedRecord) {
    return new ConsumerFailure(consumerName, attempts, event, errors, persistedRecord);
  }

  public ConsumerFailure withAttempts(final int i, JsonObject error) {
    return new ConsumerFailure(consumerName, i, event, errors.add(error), persistedRecord);
  }
}
