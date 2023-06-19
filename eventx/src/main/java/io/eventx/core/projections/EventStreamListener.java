package io.eventx.core.projections;

import io.eventx.Aggregate;
import io.eventx.LiveEventStream;
import io.eventx.core.objects.EventbusLiveProjections;
import io.eventx.infrastructure.models.Event;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EventStreamListener {

  private final Vertx vertx;
  private final Class<? extends Aggregate> aggregateClass;
  private final List<LiveEventStream> liveEventStreamConsumer;

  protected static final Logger LOGGER = LoggerFactory.getLogger(EventStreamListener.class);


  public EventStreamListener(Vertx vertx, Class<? extends Aggregate> aggregateClass, List<LiveEventStream> liveEventStreamConsumer) {
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
    this.liveEventStreamConsumer = liveEventStreamConsumer;
  }


  public Uni<Void> start() {
    return Multi.createFrom().iterable(liveEventStreamConsumer)
      .onItem().transformToUniAndMerge(consumer -> vertx.eventBus().<Event>localConsumer(EventbusLiveProjections.eventSubscriptionAddress(aggregateClass, consumer.tenant()))
        .exceptionHandler(throwable -> handle(throwable, consumer))
        .handler(eventMessage -> consumer.apply(eventMessage.body()))
        .completionHandler())
      .collect().asList()
      .replaceWithVoid();
  }

  public static void handle(Throwable throwable, LiveEventStream consumer) {
    LOGGER.error("Error in live event stream {}::{}", consumer.getClass().getName(), consumer.tenant(), throwable);
  }


}
