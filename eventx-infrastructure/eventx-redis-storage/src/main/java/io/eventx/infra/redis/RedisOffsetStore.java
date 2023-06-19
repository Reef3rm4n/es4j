package io.eventx.infra.redis;

import io.eventx.Aggregate;
import io.eventx.core.objects.JournalOffset;
import io.eventx.core.objects.JournalOffsetKey;
import io.eventx.infrastructure.OffsetStore;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

public class RedisOffsetStore implements OffsetStore {
  @Override
  public Uni<JournalOffset> put(JournalOffset journalOffset) {
    return null;
  }

  @Override
  public Uni<JournalOffset> get(JournalOffsetKey journalOffset) {
    return null;
  }

  @Override
  public Uni<Void> close() {
    return null;
  }

  @Override
  public Uni<Void> start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration) {
    return null;
  }

}
