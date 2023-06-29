package io.es4j.infrastructure;

import io.es4j.Aggregate;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.es4j.core.objects.JournalOffset;
import io.es4j.core.objects.JournalOffsetKey;
import io.vertx.mutiny.core.Vertx;

public interface OffsetStore {

  Uni<JournalOffset> put(JournalOffset journalOffset);
  Uni<JournalOffset> get(JournalOffsetKey journalOffset);

  Uni<JournalOffset> reset(JournalOffset journalOffset);

  Uni<Void> stop();
  void start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration);
  Uni<Void> setup(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration);
}
