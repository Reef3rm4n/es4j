package io.vertx.eventx.infrastructure.pg;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infrastructure.EventJournal;
import io.vertx.eventx.infrastructure.models.AppendInstruction;
import io.vertx.eventx.infrastructure.models.Event;
import io.vertx.eventx.infrastructure.models.StreamInstruction;
import io.vertx.eventx.infrastructure.pg.models.EventRecord;
import io.vertx.eventx.infrastructure.pg.models.EventRecordKey;
import io.vertx.eventx.infrastructure.pg.models.EventRecordQuery;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.QueryOptions;

import java.util.List;

public class PgEventJournal implements EventJournal {

  private final Repository<EventRecordKey, EventRecord, EventRecordQuery> eventJournal;

  public PgEventJournal(Repository<EventRecordKey, EventRecord, EventRecordQuery> eventJournal) {
    this.eventJournal = eventJournal;
  }

  @Override
  public <T extends Aggregate> Uni<List<Event>> stream(StreamInstruction<T> streamInstruction) {
    return eventJournal.query(eventJournalQuery(streamInstruction))
      .map(eventRecords -> eventRecords.stream()
        .map(eventRecord -> new Event(
            eventRecord.entityId(),
            eventRecord.eventClass(),
            eventRecord.eventVersion(),
            eventRecord.event(),
            eventRecord.baseRecord().tenantId()
          )
        ).toList()
      );
  }

  @Override
  public <T extends Aggregate> Uni<Void> append(AppendInstruction<T> appendInstruction) {
    return eventJournal.insertBatch(parseInstruction(appendInstruction));
  }

  private <T extends Aggregate> List<EventRecord> parseInstruction(AppendInstruction<T> appendInstruction) {
    return appendInstruction.events().stream()
      .map(event -> new EventRecord(
          event.entityId(),
          event.eventClass(),
          event.eventVersion(),
          event.event(),
          BaseRecord.newRecord(event.tenantId())
        )
      ).toList();
  }

  private <T extends Aggregate> EventRecordQuery eventJournalQuery(StreamInstruction<T> streamInstruction) {
    return new EventRecordQuery(
      List.of(streamInstruction.aggregateId()),
      null,
      streamInstruction.startFromVersion(),
      null,
      null,
      null,
      null,
      new QueryOptions(
        "event_version",
        false,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        streamInstruction.tenantId()
      )
    );
  }

}
