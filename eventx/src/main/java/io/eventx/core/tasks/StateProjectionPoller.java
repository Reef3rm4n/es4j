package io.eventx.core.tasks;

import io.eventx.Aggregate;
import io.eventx.core.objects.CommandHeaders;
import io.eventx.infrastructure.EventStore;
import io.eventx.infrastructure.OffsetStore;
import io.eventx.infrastructure.models.EventStreamBuilder;
import io.eventx.sql.exceptions.NotFound;
import io.eventx.task.CronTask;
import io.eventx.task.CronTaskConfiguration;
import io.eventx.task.CronTaskConfigurationBuilder;
import io.eventx.task.LockLevel;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.eventx.core.objects.LoadAggregate;
import io.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.eventx.core.objects.JournalOffsetKey;
import io.eventx.core.objects.StateProjectionWrapper;

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
    // polling would have to be done one aggregate at the time
    // polling will have to be moved to a queue
    // a new entry must be inserted in the queue for each one of the updated streams
    stateProjectionWrapper.logger().debug("Polling events");
    return offsetStore.get(new JournalOffsetKey(stateProjectionWrapper.pollingStateProjection().getClass().getName(), "default"))
      .flatMap(journalOffset -> {
          stateProjectionWrapper.logger().debug("Journal idOffset at {}", journalOffset.idOffSet());
          return eventStore.fetch(EventStreamBuilder.builder()
            .offset(journalOffset.idOffSet())
            .batchSize(5000)
            .build()
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
              tuple2 -> proxy.proxyCommand(new LoadAggregate(
                    tuple2.getItem1(),
                  tuple2.getItem2(),
                    null,
                    null,
                    CommandHeaders.defaultHeaders()
                  )
                )
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
      .cron(stateProjectionWrapper.pollingStateProjection().pollingPolicy())
      .build();
  }

}
