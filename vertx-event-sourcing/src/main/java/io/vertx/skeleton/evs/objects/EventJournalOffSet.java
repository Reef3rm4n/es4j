package io.vertx.skeleton.evs.objects;


import io.vertx.skeleton.models.PersistedRecord;
import io.vertx.skeleton.models.RepositoryRecord;

import java.time.Instant;

public record EventJournalOffSet(
  String consumer,
  Long idOffSet,
  Instant dateOffSet,
  PersistedRecord persistedRecord
) implements RepositoryRecord<EventJournalOffSet> {


  @Override
  public EventJournalOffSet with(PersistedRecord persistedRecord) {
    return new EventJournalOffSet(consumer,  idOffSet, dateOffSet, persistedRecord);
  }

  public EventJournalOffSet withIdOffSet(Long maxEventId) {
    return new EventJournalOffSet(consumer, maxEventId, dateOffSet, persistedRecord);
  }
}
