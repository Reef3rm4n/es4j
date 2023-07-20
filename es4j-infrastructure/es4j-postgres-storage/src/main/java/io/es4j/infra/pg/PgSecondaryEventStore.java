package io.es4j.infra.pg;

import io.es4j.Aggregate;
import io.es4j.Es4jDeployment;
import io.es4j.infra.pg.mappers.EventStoreMapper;
import io.es4j.infra.pg.models.EventRecord;
import io.es4j.infra.pg.models.EventRecordKey;
import io.es4j.infra.pg.models.EventRecordQuery;
import io.es4j.infrastructure.SecondaryEventStore;
import io.es4j.infrastructure.models.*;
import io.es4j.sql.LiquibaseHandler;
import io.es4j.sql.Repository;
import io.es4j.sql.RepositoryHandler;
import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.QueryOptions;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.es4j.core.CommandHandler.camelToKebab;

public class PgSecondaryEventStore implements SecondaryEventStore {

  private Repository<EventRecordKey, EventRecord, EventRecordQuery> eventJournal;
  private final Logger LOGGER = LoggerFactory.getLogger(PgEventStore.class);


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
  public Uni<Void> stop() {
    return eventJournal.repositoryHandler().close();
  }

  @Override
  public void start(Es4jDeployment es4jDeployment, Vertx vertx, JsonObject configuration) {
    this.eventJournal = new Repository<>(EventStoreMapper.INSTANCE, RepositoryHandler.leasePool(configuration, vertx));

  }

  @Override
  public Uni<Void> setup(Es4jDeployment es4jDeployment, Vertx vertx, JsonObject configuration) {
    LOGGER.debug("Migrating database for {} with configuration {}", es4jDeployment.aggregateClass().getSimpleName(), configuration);
    return LiquibaseHandler.liquibaseString(
      eventJournal.repositoryHandler(),
      "pg-event-store.xml",
      Map.of("schema", camelToKebab(es4jDeployment.aggregateClass().getSimpleName()))
    );
  }

  private <T extends Aggregate> List<EventRecord> parseInstruction(AppendInstruction<T> appendInstruction) {
    return appendInstruction.events().stream()
      .map(event -> new EventRecord(
          event.aggregateId(),
          event.eventType(),
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
      null,
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
    if (aggregateEventStream.startFromSnapshot()) {
      return "(select coalesce(max(id),0) from event_store where event_class = 'snapshot' and aggregate_id ilike any(#{aggregate_id}) and tenant = #{tenant})";
    }  else {
      return null;
    }
  }

  private EventRecordQuery eventJournalQuery(EventStream eventStream) {
    return new EventRecordQuery(
      eventStream.aggregateIds(),
      eventStream.eventTypes(),
      null,
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
