package io.eventx.infra.pg;

import io.eventx.Aggregate;
import io.eventx.infra.pg.mappers.EventStoreMapper;
import io.eventx.infra.pg.models.EventRecord;
import io.eventx.infra.pg.models.EventRecordKey;
import io.eventx.infra.pg.models.EventRecordQuery;
import io.eventx.infrastructure.SecondaryEventStore;
import io.eventx.infrastructure.models.*;
import io.eventx.sql.LiquibaseHandler;
import io.eventx.sql.Repository;
import io.eventx.sql.exceptions.NotFound;
import io.eventx.sql.models.BaseRecord;
import io.eventx.sql.models.QueryOptions;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.eventx.core.CommandHandler.camelToKebab;

public class PgSecondaryEventStore implements SecondaryEventStore {

  private final Repository<EventRecordKey, EventRecord, EventRecordQuery> eventJournal;
  private final Logger LOGGER = LoggerFactory.getLogger(PgEventStore.class);

  public PgSecondaryEventStore(Repository<EventRecordKey, EventRecord, EventRecordQuery> eventJournal) {
    this.eventJournal = eventJournal;
  }

  @Override
  public <T extends Aggregate> Uni<List<Event>> fetch(AggregateEventStream<T> aggregateEventStream) {
    return eventJournal.query(eventJournalQuery(aggregateEventStream))
      .onFailure(NotFound.class).recoverWithItem(new ArrayList<>())
      .map(eventRecords -> eventRecords.stream()
        .map(eventRecord -> new Event(
            eventRecord.aggregateId(),
            eventRecord.eventClass(),
            eventRecord.eventVersion(),
            eventRecord.event(),
            eventRecord.baseRecord().tenant(),
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
          eventRecord.aggregateId(),
          eventRecord.eventClass(),
          eventRecord.eventVersion(),
          eventRecord.event(),
          eventRecord.baseRecord().tenant(),
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
          eventRecord.aggregateId(),
          eventRecord.eventClass(),
          eventRecord.eventVersion(),
          eventRecord.event(),
          eventRecord.baseRecord().tenant(),
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
            eventRecord.id(),
            eventRecord.aggregateId(),
            eventRecord.eventClass(),
            eventRecord.eventVersion(),
            eventRecord.event(),
            eventRecord.baseRecord().tenant(),
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
  public Uni<Void> start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration) {
    LOGGER.debug("Migrating database for {} with configuration {}", aggregateClass.getSimpleName(), configuration);
    return LiquibaseHandler.liquibaseString(
      eventJournal.repositoryHandler(),
      "pg-event-store.xml",
      Map.of("schema", camelToKebab(aggregateClass.getSimpleName()))
    );
  }

  private <T extends Aggregate> List<EventRecord> parseInstruction(AppendInstruction<T> appendInstruction) {
    return appendInstruction.events().stream()
      .map(event -> new EventRecord(
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
        0,
        aggregateEventStream.maxSize(),
        startingOffset(aggregateEventStream),
        aggregateEventStream.tenantId()
      )
    );
  }

  private static <T extends Aggregate> String startingOffset(AggregateEventStream<T> aggregateEventStream) {
    if (aggregateEventStream.journalOffset() != null && aggregateEventStream.journalOffset() == 0) {
      return String.valueOf(0);
    } else if (aggregateEventStream.startFrom() != null) {
      return "(select max(id) from event_journal where event_class = '" + aggregateEventStream.startFrom().getName() + "' and aggregateId = '" + aggregateEventStream.aggregateId() + "')";
    } else {
      return null;
    }
  }

  private EventRecordQuery eventJournalQuery(EventStream eventStream) {
    return new EventRecordQuery(
      eventStream.aggregateIds(),
      eventStream.events() != null ? eventStream.events().stream().map(Class::getName).toList() : null,
      eventStream.aggregates() != null ? eventStream.aggregates().stream().map(Class::getName).toList() : null,
      eventStream.tags(),
      null,
      eventStream.versionTo(),
      eventStream.offset(),
      null,
      new QueryOptions(
        EventStoreMapper.ID,
        false,
        eventStream.from(),
        eventStream.to(),
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
