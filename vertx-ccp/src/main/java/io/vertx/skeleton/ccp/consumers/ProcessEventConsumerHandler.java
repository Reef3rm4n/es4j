package io.vertx.skeleton.ccp.consumers;

import io.vertx.skeleton.ccp.ProcessEventConsumer;
import io.vertx.skeleton.ccp.models.QueueEvent;
import io.vertx.skeleton.models.Tenant;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

public class ProcessEventConsumerHandler<T, R> {

  private final Logger logger;
  private final Class<T> payloadClass;
  private final List<ProcessEventConsumer<T, R>> consumers;

  public ProcessEventConsumerHandler(Logger logger, Class<T> payloadClass, List<ProcessEventConsumer<T, R>> consumers) {
    this.logger = logger;
    this.payloadClass = payloadClass;
    this.consumers = consumers;
  }

  public Uni<Void> consumeEvents(List<QueueEvent<T, R>> events) {
    try {
      logger.debug("Events produced-> " + events);
      final var tenantGrouped = events.stream().collect(groupingBy(QueueEvent::tenant));
      if (!consumers.isEmpty()) {
        return Multi.createFrom().iterable(tenantGrouped.entrySet())
          .onItem().transformToUniAndMerge(
            entry -> Multi.createFrom().iterable(findConsumers(entry.getKey()))
              .onItem().transformToUniAndMerge(c -> c.consume(entry.getValue())
                .onFailure()
                .invoke(throwable -> logger.error("Consumer failed -> " + c.getClass().getName(), throwable))
                .onFailure()
                .recoverWithNull()
              )
              .collect().asList()
          )
          .collect().asList()
          .replaceWithVoid();
      }
    } catch (Exception e) {
      logger.error("[-- ProcessEventConsumer had to drop the following exception --]", e);
      return Uni.createFrom().voidItem();
    }
    return Uni.createFrom().voidItem();
  }

  public List<ProcessEventConsumer<T, R>> findConsumers(Tenant tenant) {
    final var customEventConsumers = consumers.stream()
      .filter(t -> Objects.nonNull(t.tenants()))
      .filter(task -> task.tenants().stream().anyMatch(t -> t.equals(tenant)));
    final var baselineConsumers = consumers.stream().filter(task -> task.tenants() == null);
    return Stream.concat(customEventConsumers, baselineConsumers).toList();
  }

}
