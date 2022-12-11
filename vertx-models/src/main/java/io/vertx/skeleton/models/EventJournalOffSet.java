package io.vertx.skeleton.models;


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
