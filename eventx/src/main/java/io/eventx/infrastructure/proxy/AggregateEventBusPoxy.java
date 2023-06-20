package io.eventx.infrastructure.proxy;

import io.eventx.Aggregate;
import io.eventx.Command;
import io.eventx.core.objects.*;
import io.eventx.infrastructure.bus.AggregateBus;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.function.Consumer;


public class AggregateEventBusPoxy<T extends Aggregate> {
  private final Vertx vertx;
  public final Class<T> aggregateClass;
  public static final Logger LOGGER = LoggerFactory.getLogger(AggregateEventBusPoxy.class);

  public AggregateEventBusPoxy(
    final Vertx vertx,
    final Class<T> aggregateClass
  ) {
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
  }


  public Uni<AggregateState<T>> forward(Command command) {
    return AggregateBus.request(
      vertx,
      aggregateClass,
      command
    );
  }

  public Uni<Void> subscribe(Consumer<AggregateState<T>> consumer) {
    final var address = EventbusLiveProjections.subscriptionAddress(aggregateClass, "default");
    LOGGER.debug("Subscribing to state updates for {} in address {}", aggregateClass.getSimpleName(), address);
    return vertx.eventBus().<JsonObject>consumer(address).handler(jsonObjectMessage -> {
        LOGGER.debug("{} subscription incoming event {} {} ", aggregateClass.getSimpleName(), jsonObjectMessage.headers(), jsonObjectMessage.body());
        final var aggregateState = AggregateState.fromJson(jsonObjectMessage.body(), aggregateClass);
        consumer.accept(aggregateState);
      })
      .exceptionHandler(this::subscriptionError)
      .completionHandler();
  }

  public Uni<Void> subscribe(Consumer<AggregateState<T>> consumer, String tenantId) {
    final var address = EventbusLiveProjections.subscriptionAddress(aggregateClass, tenantId);
    LOGGER.debug("Subscribing to state updates for {} in address {}", aggregateClass.getSimpleName(), address);
    return vertx.eventBus().<JsonObject>consumer(address).handler(jsonObjectMessage -> {
        LOGGER.debug("{} subscription incoming event {} {} ", aggregateClass.getSimpleName(), jsonObjectMessage.headers(), jsonObjectMessage.body());
        final var aggregateState = AggregateState.fromJson(jsonObjectMessage.body(), aggregateClass);
        consumer.accept(aggregateState);
      })
      .exceptionHandler(this::subscriptionError)
      .completionHandler();
  }

  public Uni<Void> eventSubscribe(Consumer<AggregateEvent> consumer, String tenantId) {
    final var address = EventbusLiveProjections.eventSubscriptionAddress(aggregateClass, tenantId);
    LOGGER.debug("Subscribing to event stream for {} in address {}", aggregateClass.getSimpleName(), address);
    return vertx.eventBus().<JsonObject>consumer(address).handler(jsonObjectMessage -> {
        LOGGER.debug("{} subscription incoming event {} {} ", aggregateClass.getSimpleName(), jsonObjectMessage.headers(), jsonObjectMessage.body());
        final var aggregateState = jsonObjectMessage.body().mapTo(AggregateEvent.class);
        consumer.accept(aggregateState);
      })
      .exceptionHandler(this::subscriptionError)
      .completionHandler();
  }

  public Uni<Void> eventSubscribe(Consumer<AggregateEvent> consumer) {
    final var address = EventbusLiveProjections.eventSubscriptionAddress(aggregateClass, "default");
    LOGGER.debug("Subscribing to event stream for {} in address {}", aggregateClass.getSimpleName(), address);
    return vertx.eventBus().<JsonObject>consumer(address).handler(jsonObjectMessage -> {
        LOGGER.debug("{} subscription incoming event {} {} ", aggregateClass.getSimpleName(), jsonObjectMessage.headers(), jsonObjectMessage.body());

      })
      .exceptionHandler(this::subscriptionError)
      .completionHandler();
  }

  private void subscriptionError(Throwable throwable) {
    LOGGER.error("{} subscription dropped exception", aggregateClass, throwable);
  }

  public record CommandWrapper(
    String commandClass,
    Command command
  ) {
  }


}
