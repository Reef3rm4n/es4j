package io.es4j.infra.redis;

import com.google.auto.service.AutoService;
import io.es4j.Aggregate;
import io.es4j.core.objects.JournalOffset;
import io.es4j.core.objects.JournalOffsetKey;
import io.es4j.infrastructure.OffsetStore;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;


@AutoService(OffsetStore.class)
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
  public Uni<JournalOffset> reset(JournalOffset journalOffset) {
    return null;
  }

  @Override
  public Uni<Void> stop() {
    return null;
  }

  @Override
  public void start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration) {

  }

  @Override
  public Uni<Void> setup(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration) {
    return null;
  }

}
