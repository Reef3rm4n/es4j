package io.eventx.core.tasks;

import io.eventx.core.objects.AggregateEvent;
import io.eventx.infrastructure.EventStore;
import io.eventx.infrastructure.OffsetStore;
import io.eventx.infrastructure.models.Event;
import io.eventx.sql.exceptions.NotFound;
import io.eventx.task.CronTask;
import io.eventx.task.CronTaskConfiguration;
import io.eventx.task.CronTaskConfigurationBuilder;
import io.eventx.task.LockLevel;
import io.smallrye.mutiny.Uni;
import io.eventx.EventProjection;
import io.eventx.infrastructure.misc.EventParser;
import io.eventx.infrastructure.models.EventStream;
import io.eventx.core.objects.JournalOffset;
import io.eventx.core.objects.JournalOffsetKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


public class EventProjectionPoller implements CronTask {

  private static final Logger logger = LoggerFactory.getLogger(EventProjectionPoller.class);
  private final EventProjection eventProjection;
  private final EventStore eventStore;
  private final OffsetStore offsetStore;

  public EventProjectionPoller(
    EventProjection eventProjections,
    EventStore eventStore,
    OffsetStore offsetStore
  ) {
    this.eventProjection = eventProjections;
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
  }

  @Override
  public Uni<Void> performTask() {
    return offsetStore.get(getOffset())
      .flatMap(journalOffset -> eventStore.fetch(streamStatement(eventProjection, journalOffset))
        .flatMap(events -> eventProjection.apply(parseEvents(events))
          .flatMap(avoid -> offsetStore.put(journalOffset.updateOffset(events)))
        )
      )
      .onFailure().invoke(throwable -> logger.error("Unable to update projection {}", eventProjection.getClass().getName(), throwable))
      .replaceWithVoid();
  }

  private JournalOffsetKey getOffset() {
    return new JournalOffsetKey(eventProjection.getClass().getName(), eventProjection.tenantID());
  }

  private static EventStream streamStatement(EventProjection eventProjection, JournalOffset journalOffset) {
    AtomicReference<EventStream> eventStream = new AtomicReference<>();
    eventProjection.filter().ifPresentOrElse(
      filter -> eventStream.set(new EventStream(
        null,
        filter.events(),
        null,
        filter.tags(),
        eventProjection.tenantID(),
        journalOffset.idOffSet(),
        1000,
        null,
        null,
        null,
        null
      )),
      () -> eventStream.set(
        new EventStream(
          null,
          null,
          null,
          null,
          eventProjection.tenantID(),
          journalOffset.idOffSet(),
          1000,
          null,
          null,
          null,
          null
        )
      )
    );
    return eventStream.get();
  }


  private List<AggregateEvent> parseEvents(List<Event> events) {
    return events.stream()
      .map(event -> new AggregateEvent(
          event.aggregateId(),
          event.tenantId(),
          event.journalOffset(),
          event.eventVersion(),
          EventParser.getEvent(event.eventClass(), event.event())
        )
      )
      .toList();
  }

  @Override
  public CronTaskConfiguration configuration() {
    return CronTaskConfigurationBuilder.builder()
      .knownInterruptions(List.of(NotFound.class))
      .lockLevel(LockLevel.CLUSTER_WIDE)
      .cron(eventProjection.pollingPolicy())
      .build();
  }

}
