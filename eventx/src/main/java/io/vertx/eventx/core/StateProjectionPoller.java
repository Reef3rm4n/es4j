package io.vertx.eventx.core;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.eventx.sql.exceptions.NotFound;
import io.vertx.eventx.task.*;
import io.vertx.eventx.infrastructure.EventStore;
import io.vertx.eventx.infrastructure.OffsetStore;
import io.vertx.eventx.infrastructure.models.AggregatePlainKey;
import io.vertx.eventx.infrastructure.models.EventStream;
import io.vertx.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.vertx.eventx.objects.JournalOffsetKey;
import io.vertx.eventx.objects.StateProjectionWrapper;
import io.vertx.eventx.Aggregate;

import java.time.Duration;
import java.util.List;

import static java.util.stream.Collectors.groupingBy;

public class StateProjectionPoller<T extends Aggregate> implements CronTask {

  private final StateProjectionWrapper<T> stateProjectionWrapper;
  private final AggregateEventBusPoxy<T> proxy;
  private final EventStore eventStore;
  private final OffsetStore offsetStore;
  private final Class<T> aggregateClass;

  public StateProjectionPoller(
    final Class<T> aggregateClass,
    final StateProjectionWrapper<T> stateProjectionWrapper,
    final AggregateEventBusPoxy<T> proxy,
    final EventStore eventStore,
    final OffsetStore offsetStore
  ) {
    this.aggregateClass = aggregateClass;
    this.stateProjectionWrapper = stateProjectionWrapper;
    this.proxy = proxy;
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
  }

  @Override
  public Uni<Void> performTask() {
    stateProjectionWrapper.logger().debug("Polling events");
    return offsetStore.get(new JournalOffsetKey(stateProjectionWrapper.stateProjection().getClass().getName(), "default"))
      .flatMap(journalOffset -> {
          stateProjectionWrapper.logger().debug("ID offset marked at {}", journalOffset.idOffSet());
          return eventStore.fetch(new EventStream(
              List.of(aggregateClass),
              null,
              null,
              null,
              null,
              journalOffset.idOffSet(),
              1000
            )
          );
        }
      )
      .flatMap(events -> {
          final var aggregateIds = events.stream()
            .collect(groupingBy(event -> Tuple2.of(event.aggregateId(), event.tenantId())))
            .keySet();
          stateProjectionWrapper.logger().debug("Updating {} IDs : {}", aggregateClass.getSimpleName(), aggregateIds);
          return Multi.createFrom().iterable(aggregateIds)
            .onItem().transformToUniAndMerge(
              tuple2 -> proxy.wakeUp(new AggregatePlainKey(aggregateClass.getName(), tuple2.getItem1(), tuple2.getItem2()))
                .flatMap(stateProjectionWrapper::update)
            )
            .collect().asList()
            .replaceWithVoid();
        }
      )
      .replaceWithVoid();
  }

  @Override
  public CronTaskConfiguration configuration() {
    return CronTaskConfigurationBuilder.builder()
      .knownInterruptions(List.of(NotFound.class))
      .lockLevel(LockLevel.CLUSTER_WIDE)
      .cron(stateProjectionWrapper.stateProjection().pollingPolicy())
      .build();
  }

}
