package io.eventx.infrastructure;

import io.eventx.Aggregate;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.eventx.core.objects.JournalOffset;
import io.eventx.core.objects.JournalOffsetKey;
import io.vertx.mutiny.core.Vertx;

public interface OffsetStore {

  Uni<JournalOffset> put(JournalOffset journalOffset);
  Uni<JournalOffset> get(JournalOffsetKey journalOffset);

  Uni<Void> stop();


  void start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration);
  Uni<Void> setup(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration);
}
