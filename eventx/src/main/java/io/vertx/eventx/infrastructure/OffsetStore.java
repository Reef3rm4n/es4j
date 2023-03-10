package io.vertx.eventx.infrastructure;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.objects.JournalOffset;
import io.vertx.eventx.objects.JournalOffsetKey;

public interface OffsetStore {

  Uni<JournalOffset> put(JournalOffset journalOffset);
  Uni<JournalOffset> get(JournalOffsetKey journalOffset);

  Uni<Void> close();

  Uni<Void> start();
}
