package io.vertx.eventx.infrastructure;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.core.objects.JournalOffset;
import io.vertx.eventx.core.objects.JournalOffsetKey;
import io.vertx.mutiny.core.Vertx;

public interface OffsetStore {

  Uni<JournalOffset> put(JournalOffset journalOffset);
  Uni<JournalOffset> get(JournalOffsetKey journalOffset);

  Uni<Void> close();


  Uni<Void> start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration);
}
