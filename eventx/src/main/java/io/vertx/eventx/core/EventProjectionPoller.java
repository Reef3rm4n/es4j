package io.vertx.eventx.core;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.eventx.EventProjection;
import io.vertx.eventx.infrastructure.EventStore;
import io.vertx.eventx.infrastructure.OffsetStore;
import io.vertx.eventx.infrastructure.misc.EventParser;
import io.vertx.eventx.infrastructure.models.EventStream;
import io.vertx.eventx.objects.JournalOffset;
import io.vertx.eventx.objects.JournalOffsetKey;
import io.vertx.eventx.objects.PolledEvent;
import io.vertx.eventx.sql.exceptions.NotFound;
import io.vertx.eventx.task.LockLevel;
import io.vertx.eventx.task.TimerTask;
import io.vertx.eventx.task.TimerTaskConfiguration;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


public class EventProjectionPoller implements TimerTask {

  private static final Logger logger = LoggerFactory.getLogger(EventProjectionPoller.class);
  private final List<EventProjection> eventProjections;
  private final EventStore eventStore;
  private final OffsetStore offsetStore;

  public EventProjectionPoller(
    List<EventProjection> eventProjections,
    EventStore eventStore,
    OffsetStore offsetStore
  ) {
    this.eventProjections = eventProjections;
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
  }

  @Override
  public Uni<Void> performTask() {
    return Multi.createFrom().iterable(eventProjections)
      .onItem().transformToUniAndMerge(eventProjection -> offsetStore.get(new JournalOffsetKey(eventProjection.getClass().getName(), eventProjection.tenantID()))
        .flatMap(journalOffset -> eventStore.fetch(streamStatement(eventProjection, journalOffset))
            .flatMap(
              events -> eventProjection.apply(parseEvents(events))
                .flatMap(avoid -> offsetStore.put(journalOffset.updateOffset(events)))
            )
        )
        .onFailure().invoke(throwable -> logger.error("Failure during projection update", throwable))
        .onFailure().recoverWithNull()
      )
      .collect().asList()
      .replaceWithVoid();
  }


  private static EventStream streamStatement(EventProjection eventProjection, JournalOffset journalOffset) {
    AtomicReference<EventStream> eventStream = new AtomicReference<>();
    eventProjection.filter().ifPresentOrElse(
      filter ->  {
        eventStream.set(new EventStream(
          filter.aggregates(),
          filter.events(),
          null,
          filter.tags(),
          eventProjection.tenantID(),
          journalOffset.idOffSet(),
          1000
        ));
      },
      () -> eventStream.set(
        new EventStream(
          null,
           null,
          null,
         null,
          eventProjection.tenantID(),
          journalOffset.idOffSet(),
          1000
        )
      )
    );
    return eventStream.get();
  }


  private List<PolledEvent> parseEvents(List<io.vertx.eventx.infrastructure.models.Event> events) {
    return events.stream()
      .map(event -> new PolledEvent(
        event.aggregateClass(),
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
  public TimerTaskConfiguration configuration() {
    return new TimerTaskConfiguration(
      LockLevel.CLUSTER_WIDE,
      100_000L,
      100_000L,
      10L,
      10L,
      List.of(NotFound.class)
    );
  }


}
