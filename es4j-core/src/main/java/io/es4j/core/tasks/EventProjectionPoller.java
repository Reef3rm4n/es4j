package io.es4j.core.tasks;


import io.es4j.core.objects.AggregateEvent;
import io.es4j.core.objects.Offset;
import io.es4j.core.objects.OffsetKey;
import io.es4j.infrastructure.EventStore;
import io.es4j.infrastructure.OffsetStore;
import io.es4j.infrastructure.models.Event;
import io.es4j.infrastructure.models.EventStreamBuilder;
import io.es4j.sql.exceptions.NotFound;
import io.es4j.task.CronTask;
import io.es4j.task.CronTaskConfiguration;
import io.es4j.task.CronTaskConfigurationBuilder;
import io.es4j.task.LockLevel;
import io.smallrye.mutiny.Uni;
import io.es4j.PollingEventProjection;
import io.es4j.infrastructure.misc.EventParser;
import io.es4j.infrastructure.models.EventStream;

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

  private OffsetKey getOffset() {
    return new OffsetKey(pollingEventProjection.getClass().getName(), pollingEventProjection.tenant());
  }

  private static EventStream streamStatement(PollingEventProjection pollingEventProjection, Offset offset) {
    AtomicReference<EventStream> eventStream = new AtomicReference<>();
    pollingEventProjection.filter().ifPresentOrElse(
      filter -> eventStream.set(
        EventStreamBuilder.builder()
          .eventTypes(filter.eventTypes())
          .tenantId(pollingEventProjection.tenant())
          .offset(offset.idOffSet())
          .batchSize(1000)
          .tags(filter.tags())
          .build()
      ),
      () -> eventStream.set(
        EventStreamBuilder.builder()
          .tenantId(pollingEventProjection.tenant())
          .offset(offset.idOffSet())
          .batchSize(1000)
          .build()
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
          EventParser.getEvent(event.eventType(), event.event())
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
