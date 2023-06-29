package io.es4j.infra.pg.models;

import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;

public record EventJournalOffSet(
  String consumer,
  Long idOffSet,
  Long eventVersionOffset,
  BaseRecord baseRecord
) implements RepositoryRecord<EventJournalOffSet> {


  @Override
  public EventJournalOffSet with(BaseRecord persistedRecord) {
    return new EventJournalOffSet(consumer, idOffSet, eventVersionOffset, persistedRecord);
  }

  public EventJournalOffSet withIdOffSet(Long maxEventId) {
    return new EventJournalOffSet(consumer, maxEventId, eventVersionOffset, baseRecord);
  }
}
