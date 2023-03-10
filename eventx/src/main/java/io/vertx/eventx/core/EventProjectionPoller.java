package io.vertx.eventx.core;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.eventx.Event;
import io.vertx.eventx.EventProjection;
import io.vertx.eventx.infrastructure.EventStore;
import io.vertx.eventx.infrastructure.OffsetStore;
import io.vertx.eventx.infrastructure.misc.EventParser;
import io.vertx.eventx.infrastructure.models.EventStream;
import io.vertx.eventx.objects.JournalOffsetKey;
import io.vertx.eventx.sql.exceptions.NotFound;
import io.vertx.eventx.task.LockLevel;
import io.vertx.eventx.task.TimerTask;
import io.vertx.eventx.task.TimerTaskConfiguration;

import java.util.List;


public class EventProjectionPoller implements TimerTask {

  private static final Logger logger = LoggerFactory.getLogger(EventProjectionPoller.class);
  private final List<EventProjection> eventProjections;
  private final EventStore eventStore;
  private final OffsetStore offsetStore;

  public EventProjectionPoller(List<EventProjection> eventProjections, EventStore eventStore, OffsetStore offsetStore) {
    this.eventProjections = eventProjections;
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
  }


  //
  @Override
  public Uni<Void> performTask() {
    return Multi.createFrom().iterable(eventProjections)
      .onItem().transformToUniAndMerge(eventProjection -> offsetStore.get(new JournalOffsetKey(eventProjection.getClass().getName(), eventProjection.tenantID()))
        .flatMap(journalOffset -> eventStore.fetch(new EventStream(
                eventProjection.filter().aggregates(),
                eventProjection.filter().events(),
                null,
                eventProjection.filter().tags(),
                eventProjection.tenantID(),
                journalOffset.idOffSet(),
                1000
              )
            )
            .flatMap(
              events -> eventProjection.apply(parseEvents(events))
                .flatMap(avoid -> offsetStore.put(journalOffset.updateOffset(events)))
            )
        )
      )
      .collect().asList()
      .replaceWithVoid();
  }


  private List<Event> parseEvents(List<io.vertx.eventx.infrastructure.models.Event> events) {
    return events.stream()
      .map(event -> (Event) EventParser.getEvent(event.eventClass(), event.event()))
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
