package io.vertx.eventx.core;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.eventx.infrastructure.EventStore;
import io.vertx.eventx.infrastructure.OffsetStore;
import io.vertx.eventx.infrastructure.models.AggregatePlainKey;
import io.vertx.eventx.infrastructure.models.EventStream;
import io.vertx.eventx.infrastructure.proxies.AggregateEventBusPoxy;
import io.vertx.eventx.objects.JournalOffsetKey;
import io.vertx.eventx.objects.StateProjectionWrapper;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.sql.exceptions.NotFound;
import io.vertx.eventx.task.LockLevel;
import io.vertx.eventx.task.TimerTask;
import io.vertx.eventx.task.TimerTaskConfiguration;

import java.util.List;

import static java.util.stream.Collectors.groupingBy;

public class StateProjectionPoller<T extends Aggregate> implements TimerTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateProjectionPoller.class);
  private final List<StateProjectionWrapper<T>> stateProjectionWrappers;
  private final AggregateEventBusPoxy<T> proxy;
  private final EventStore eventStore;
  private final OffsetStore offsetStore;
  Class<T> aggregateClass;

  public StateProjectionPoller(
    List<StateProjectionWrapper<T>> stateProjectionWrappers,
    AggregateEventBusPoxy<T> proxy,
    EventStore eventStore,
    OffsetStore offsetStore
  ) {
    this.stateProjectionWrappers = stateProjectionWrappers;
    this.proxy = proxy;
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
  }

  @Override
  public Uni<Void> performTask() {
    return offsetStore.get(new JournalOffsetKey(proxy.aggregateClass.getName(), "default"))
      .flatMap(journalOffset -> eventStore.fetch(new EventStream(
              List.of(aggregateClass),
              null,
              null,
              null,
              null,
              journalOffset.idOffSet(),
              1000
            )
          )
          .flatMap(events -> {
              final var aggregateIds = events.stream()
                .collect(groupingBy(event -> Tuple2.of(event.aggregateId(),event.tenantId())))
                .keySet();
              return Multi.createFrom().iterable(aggregateIds)
                .onItem().transformToUniAndMerge(tuple2 -> proxy.wakeUp(new AggregatePlainKey(aggregateClass.getName(), tuple2.getItem1(), tuple2.getItem2()))
                  .flatMap(state -> Multi.createFrom().iterable(stateProjectionWrappers)
                    .onItem().transformToUniAndMerge(stateProjection -> stateProjection.update(state))
                    .collect().asList()
                  )
                ).collect().asList()
                .replaceWithVoid();
            }
          )
          .replaceWithVoid()
      );
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
