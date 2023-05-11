package io.vertx.eventx.infra.pg;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infrastructure.EventStore;
import io.vertx.eventx.infrastructure.models.AggregateEventStream;
import io.vertx.eventx.infrastructure.models.AppendInstruction;
import io.vertx.eventx.infrastructure.models.Event;
import io.vertx.eventx.infrastructure.models.EventStream;
import io.vertx.eventx.infra.pg.mappers.EventStoreMapper;
import io.vertx.eventx.infra.pg.models.EventRecord;
import io.vertx.eventx.infra.pg.models.EventRecordKey;
import io.vertx.eventx.infra.pg.models.EventRecordQuery;
import io.vertx.eventx.sql.LiquibaseHandler;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.exceptions.NotFound;
import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.QueryOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class PgEventStore implements EventStore {

  private final Repository<EventRecordKey, EventRecord, EventRecordQuery> eventJournal;

  public PgEventStore(Repository<EventRecordKey, EventRecord, EventRecordQuery> eventJournal) {
    this.eventJournal = eventJournal;
  }

  @Override
  public <T extends Aggregate> Uni<List<Event>> fetch(AggregateEventStream<T> aggregateEventStream) {
    return eventJournal.query(eventJournalQuery(aggregateEventStream))
      .onFailure(NotFound.class).recoverWithItem(new ArrayList<>())
      .map(eventRecords -> eventRecords.stream()
        .map(eventRecord -> new Event(
            eventRecord.aggregateClass(),
            eventRecord.aggregateId(),
            eventRecord.eventClass(),
            eventRecord.eventVersion(),
            eventRecord.event(),
            eventRecord.baseRecord().tenantId(),
            eventRecord.commandId(),
            eventRecord.tags(),
            eventRecord.schemaVersion()
          )
        ).toList()
      );
  }


  @Override
  public <T extends Aggregate> Uni<Void> stream(AggregateEventStream<T> aggregateEventStream, Consumer<Event> consumer) {
    return eventJournal.stream(
      eventRecord -> {
        final var infraEvent = new Event(
          eventRecord.aggregateClass(),
          eventRecord.aggregateId(),
          eventRecord.eventClass(),
          eventRecord.eventVersion(),
          eventRecord.event(),
          eventRecord.baseRecord().tenantId(),
          eventRecord.commandId(),
          eventRecord.tags(),
          eventRecord.schemaVersion()
        );
        consumer.accept(infraEvent);
      },
      eventJournalQuery(aggregateEventStream)
    );
  }

  @Override
  public Uni<Void> stream(EventStream eventStream, Consumer<Event> consumer) {
    return eventJournal.stream(
      eventRecord -> {
        final var infraEvent = new Event(
          eventRecord.aggregateClass(),
          eventRecord.aggregateId(),
          eventRecord.eventClass(),
          eventRecord.eventVersion(),
          eventRecord.event(),
          eventRecord.baseRecord().tenantId(),
          eventRecord.commandId(),
          eventRecord.tags(),
          eventRecord.schemaVersion()
        );
        consumer.accept(infraEvent);
      },
      eventJournalQuery(eventStream)
    );
  }

  public Uni<List<Event>> fetch(EventStream eventStream) {
    return eventJournal.query(eventJournalQuery(eventStream))
      .map(eventRecords -> eventRecords.stream()
        .map(eventRecord -> new Event(
            eventRecord.aggregateClass(),
            eventRecord.aggregateId(),
            eventRecord.eventClass(),
            eventRecord.eventVersion(),
            eventRecord.event(),
            eventRecord.baseRecord().tenantId(),
            eventRecord.commandId(),
            eventRecord.tags(),
            eventRecord.schemaVersion()
          )
        ).toList()
      );
  }

  @Override
  public <T extends Aggregate> Uni<Void> append(AppendInstruction<T> appendInstruction) {
    return eventJournal.insertBatch(parseInstruction(appendInstruction));
  }

  @Override
  public Uni<Void> close() {
    return eventJournal.repositoryHandler().close();
  }

  @Override
  public Uni<Void> start() {
    return LiquibaseHandler.runLiquibaseChangeLog(
      "pg-event-store.xml",
      eventJournal.repositoryHandler().vertx(),
      eventJournal.repositoryHandler().configuration()
    );
  }

  private <T extends Aggregate> List<EventRecord> parseInstruction(AppendInstruction<T> appendInstruction) {
    return appendInstruction.events().stream()
      .map(event -> new EventRecord(
          appendInstruction.aggregate().getName(),
          event.aggregateId(),
          event.eventClass(),
          event.eventVersion(),
          event.event(),
          event.commandId(),
          event.tags(),
          event.schemaVersion(),
          BaseRecord.newRecord(event.tenantId())
        )
      ).toList();
  }

  private <T extends Aggregate> EventRecordQuery eventJournalQuery(AggregateEventStream<T> aggregateEventStream) {
    return new EventRecordQuery(
      List.of(aggregateEventStream.aggregateId()),
      null,
      List.of(aggregateEventStream.aggregate().getName()),
      null,
      aggregateEventStream.eventVersionOffset(),
      null,
      null,
      null,
      new QueryOptions(
        EventStoreMapper.EVENT_VERSION,
        false,
        null,
        null,
        null,
        null,
        null,
        aggregateEventStream.maxSize(),
        startingOffset(aggregateEventStream),
        aggregateEventStream.tenantId()
      )
    );
  }

  private static <T extends Aggregate> String startingOffset(AggregateEventStream<T> aggregateEventStream) {
    if (aggregateEventStream.journalOffset() != null) {
      return String.valueOf(aggregateEventStream.journalOffset());
    } else if (aggregateEventStream.startFrom() != null) {
      return "(select max(id) from event_journal where event_class = '" + aggregateEventStream.startFrom().getName() + "')";
    } else {
      return null;
    }
  }

  private EventRecordQuery eventJournalQuery(EventStream eventStream) {
    return new EventRecordQuery(
      eventStream.aggregateIds(),
      eventStream.events().stream().map(Class::getName).toList(),
      eventStream.aggregates().stream().map(Class::getName).toList(),
      eventStream.tags(),
      null,
      null,
      eventStream.offset(),
      null,
      new QueryOptions(
        EventStoreMapper.ID,
        false,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        eventStream.tenantId()
      )
    );
  }


}
