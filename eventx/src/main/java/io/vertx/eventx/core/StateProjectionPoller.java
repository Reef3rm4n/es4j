package io.vertx.eventx.core;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.eventx.queue.models.QueueTransaction;
import io.vertx.eventx.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.eventx.infrastructure.EventStore;
import io.vertx.eventx.infrastructure.OffsetStore;
import io.vertx.eventx.infrastructure.models.AggregatePlainKey;
import io.vertx.eventx.infrastructure.models.EventStream;
import io.vertx.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.vertx.eventx.objects.JournalOffsetKey;
import io.vertx.eventx.objects.StateProjectionWrapper;
import io.vertx.eventx.Aggregate;

import java.util.List;

import static java.util.stream.Collectors.groupingBy;

public class StateProjectionPoller<T extends Aggregate> implements CronTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateProjectionPoller.class);
  private final List<StateProjectionWrapper<T>> stateProjectionWrappers;
  private final AggregateEventBusPoxy<T> proxy;
  private final EventStore eventStore;
  private final OffsetStore offsetStore;
  private final Class<T> aggregateClass;

  public StateProjectionPoller(
    final Class<T> aggregateClass,
    final List<StateProjectionWrapper<T>> stateProjectionWrappers,
    final AggregateEventBusPoxy<T> proxy,
    final EventStore eventStore,
    final OffsetStore offsetStore
  ) {
    this.aggregateClass = aggregateClass;
    this.stateProjectionWrappers = stateProjectionWrappers;
    this.proxy = proxy;
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
  }

  @Override
  public Uni<Void> performTask(QueueTransaction transaction) {
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
  public CronTaskConfiguration configuration() {
    return CronTaskConfigurationBuilder.builder()
      .cron(new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)).parse("*/5 * * * *"))
      .priority(0)
      .build();
  }

}
