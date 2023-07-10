package io.es4j.core.projections;

import io.es4j.Aggregate;
import io.es4j.LiveEventStream;
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
    // todo implement a catch-up mechanism
    // each aggregate stream must have an idOffset based on it's versioning
    // if current live stream version is higher than the event received than ignore
    // if current live stream version is lower than event received than shall replay previous before consuming current
    // should be able to force aggregate live stream to be reset in the database
    return Uni.createFrom().voidItem();
//    return Multi.createFrom().iterable(liveEventProjectionConsumer)
//      .onItem().transformToUniAndMerge(consumer -> vertx.eventBus().<Event>localConsumer(EventbusLiveStreams.eventLiveStream(aggregateClass, consumer.tenant()))
//        .exceptionHandler(throwable -> handle(throwable, consumer))
//        .handler(eventMessage -> consumer.apply(eventMessage.body()))
//        .completionHandler())
//      .collect().asList()
//      .replaceWithVoid();
  }

  public static void handle(Throwable throwable, LiveEventStream consumer) {
    LOGGER.error("Error in live event stream {}::{}", consumer.getClass().getName(), consumer.tenant(), throwable);
  }


}
