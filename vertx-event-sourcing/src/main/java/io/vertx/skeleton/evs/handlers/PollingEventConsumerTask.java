package io.vertx.skeleton.evs.handlers;

import io.vertx.skeleton.evs.PolledEventConsumer;
import io.vertx.skeleton.evs.objects.PolledEvent;
import io.vertx.skeleton.task.SynchronizedTask;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.models.*;
import io.vertx.skeleton.orm.Repository;
import io.vertx.skeleton.orm.RepositoryHandler;

import java.util.*;

public class PollingEventConsumerTask implements SynchronizedTask {

  private JsonObject taskConfiguration;
  private Repository<String, ConsumerFailure, EmptyQuery> consumerFailure;
  private static final Logger LOGGER = LoggerFactory.getLogger(PollingEventConsumerTask.class);

  protected RepositoryHandler repositoryHandler;
  private Repository<EventJournalOffSetKey, EventJournalOffSet, EmptyQuery> eventJournalOffset;
  private List<Repository<EntityEventKey, EntityEvent, EventJournalQuery>> eventJournalRepositories;
  private List<PolledEventConsumer> eventConsumers;
  private Map<Class<? extends PolledEventConsumer>, Logger> loggers;


//  @Override
//  public void bootstrap(final RepositoryHandler repositoryHandler, final JsonObject taskConfiguration) {
//    this.eventConsumers = CustomClassLoader.loadImplementations(PolledEventConsumer.class);
//    if (! eventConsumers.isEmpty()) {
//      eventConsumers.forEach(consumer -> {
//        LOGGER.info(consumer.getClass().getName() + " registered for events -> " + consumer.events());
//        consumer.bootstrap(repositoryHandler, taskConfiguration.getJsonObject(consumer.getClass().getSimpleName()));
//      });
//      this.eventJournalRepositories = eventConsumers.stream()
//        .map(polledEventConsumer -> Objects.requireNonNull(polledEventConsumer.eventJournal(), "Event journal table not defined for consumer -> " + polledEventConsumer.getClass().getSimpleName()))
//        .map(EventJournalMapper::new)
//        .map(mapper -> new Repository<>(mapper, repositoryHandler))
//        .toList();
//      this.loggers = eventConsumers.stream().collect(Collectors.toMap(PolledEventConsumer::getClass, consumer -> LoggerFactory.getLogger(consumer.getClass())));
//      this.eventJournalOffset = new Repository<>(EventJournalOffsetMapper.EVENT_JOURNAL_OFFSET_MAPPER, repositoryHandler);
//      this.consumerFailure = new Repository<>(ConsumerFailureMapper.EVENT_CONSUMER_FAILURE_MAPPER, repositoryHandler);
//      this.taskConfiguration = taskConfiguration;
//    }
//    LOGGER.info("No consumers registered in classpath ");
//    throw new IllegalArgumentException("No polling consumers registered");
//  }

  @Override
  public Uni<Void> performTask() {
    return Multi.createFrom().iterable(eventConsumers)
      .onItem().transformToUniAndMerge(consumer -> {
          final var logger = loggers.get(consumer.getClass());
          final var eventJournal = eventJournalRepositories.stream().filter(j -> j.mapper().table().equals(consumer.eventJournal()))
            .findFirst().orElseThrow(() -> new IllegalStateException("EventJournal not found for consumer -> " + consumer.getClass()));
          return eventJournalOffset.selectByKey(new EventJournalOffSetKey(consumer.eventJournal()))
            .onFailure().recoverWithItem(throwable -> handleOffsetFailure(throwable, consumer, logger))
            .flatMap(
              eventJournalOffSet -> eventJournal.query(getEventJournalQuery(eventJournalOffSet, consumer))
                .flatMap(events -> handleEventOffSet(eventJournalOffSet, events, consumer, logger))
            );
        }
      ).collect().asList()
      .replaceWithVoid();
  }

  private EventJournalQuery getEventJournalQuery(final EventJournalOffSet eventJournalOffSet, final PolledEventConsumer consumer) {
    return new EventJournalQuery(
      null,
      consumer.events() != null ? consumer.events().stream().map(Class::getName).toList() : null,
      null,
      new QueryOptions(
        "id",
        false,
        null,
        null,
        null,
        null,
        null,
        taskConfiguration.getInteger("eventConsumerBatchSize", 100), eventJournalOffSet.idOffSet(),
        null
      )
    );
  }


  private Uni<Void> handleEventOffSet(final EventJournalOffSet eventJournalOffSet, final List<EntityEvent> events, PolledEventConsumer polledEventConsumer, Logger logger) {
    logger.debug("Events being handled :" + events);
    final var maxEventId = events.stream().map(event -> event.persistedRecord().id()).max(Comparator.naturalOrder()).orElseThrow();
    final var minEventId = events.stream().map(event -> event.persistedRecord().id()).min(Comparator.naturalOrder()).orElseThrow();
    logger.info("Processing events from " + null + " with id from " + minEventId + " to " + maxEventId);
    final var polledEvents = events.stream().map(event -> new PolledEvent(event.entityId(), event.persistedRecord().tenant(), getEvent(event.eventClass(), event.event(), logger))).toList();
    return polledEventConsumer.consumeEvents(polledEvents)
      .onFailure().invoke(throwable -> handleConsumerFailure(throwable, polledEvents, polledEventConsumer, logger))
      .flatMap(avoid -> handleIdOffset(eventJournalOffSet, maxEventId))
      .replaceWithVoid();
  }

  private void handleConsumerFailure(final Throwable throwable, final List<PolledEvent> polledEvents, PolledEventConsumer polledEventConsumer, Logger logger) {
    logger.error("Unable to handle events -> " + polledEvents, throwable);
  }


  private Uni<EventJournalOffSet> handleIdOffset(final EventJournalOffSet eventJournalOffSet, final Long maxEventId) {
    if (eventJournalOffSet.idOffSet() == 0) {
      return eventJournalOffset.insert(eventJournalOffSet.withIdOffSet(maxEventId));
    } else {
      return eventJournalOffset.updateById(eventJournalOffSet.withIdOffSet(maxEventId));
    }
  }

  private EventJournalOffSet handleOffsetFailure(final Throwable throwable, PolledEventConsumer polledEventConsumer, Logger logger) {
    if (throwable instanceof OrmNotFoundException) {
      logger.info("Inserting new offset for user event journal -> " + polledEventConsumer.eventJournal());
      return new EventJournalOffSet(polledEventConsumer.getClass().getSimpleName(), 0L, null, PersistedRecord.tenantLess());
    } else if (throwable instanceof VertxServiceException utsException) {
      logger.error("Error fetching offset", throwable);
      throw utsException;
    } else {
      logger.error("Error fetching offset", throwable);
      throw new IllegalStateException(throwable);
    }
  }


  private Object getEvent(final String eventClazz, JsonObject event, Logger logger) {
    try {
      final var eventClass = Class.forName(eventClazz);
      return event.mapTo(eventClass);
    } catch (Exception e) {
      logger.error("Unable to cast event", e);
      throw new IllegalArgumentException("Unable to cast event");
    }
  }

}
