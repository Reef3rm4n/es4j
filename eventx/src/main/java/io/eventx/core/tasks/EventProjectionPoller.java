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
import io.eventx.PollingEventProjection;
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
  private final PollingEventProjection pollingEventProjection;
  private final EventStore eventStore;
  private final OffsetStore offsetStore;

  public EventProjectionPoller(
    PollingEventProjection pollingEventProjections,
    EventStore eventStore,
    OffsetStore offsetStore
  ) {
    this.pollingEventProjection = pollingEventProjections;
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
  }

  @Override
  public Uni<Void> performTask() {
    return offsetStore.get(getOffset())
      .flatMap(journalOffset -> eventStore.fetch(streamStatement(pollingEventProjection, journalOffset))
        .flatMap(events -> pollingEventProjection.apply(parseEvents(events))
          .flatMap(avoid -> offsetStore.put(journalOffset.updateOffset(events)))
        )
      )
      .onFailure().invoke(throwable -> logger.error("Unable to update projection {}", pollingEventProjection.getClass().getName(), throwable))
      .replaceWithVoid();
  }

  private JournalOffsetKey getOffset() {
    return new JournalOffsetKey(pollingEventProjection.getClass().getName(), pollingEventProjection.tenant());
  }

  private static EventStream streamStatement(PollingEventProjection pollingEventProjection, JournalOffset journalOffset) {
    AtomicReference<EventStream> eventStream = new AtomicReference<>();
    pollingEventProjection.filter().ifPresentOrElse(
      filter -> eventStream.set(new EventStream(
        null,
        filter.events(),
        null,
        filter.tags(),
        pollingEventProjection.tenant(),
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
          pollingEventProjection.tenant(),
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
      .cron(pollingEventProjection.pollingPolicy())
      .build();
  }

}
