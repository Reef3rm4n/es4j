package io.vertx.eventx.infrastructure.proxy;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Command;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.core.objects.*;
import io.vertx.eventx.core.projections.EventbusEventStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.core.exceptions.CommandRejected;
import io.vertx.mutiny.core.Vertx;

import java.util.Objects;
import java.util.function.Consumer;

import static io.vertx.eventx.infrastructure.bus.AggregateBus.request;


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
    return request(
      vertx,
      aggregateClass,
      JsonObject.mapFrom(new CommandWrapper(command.getClass().getName(), command))
    );
  }

  public Uni<AggregateState<T>> forward(final JsonObject command) {
    validateClass(command);
    validateCommand(command);
    return request(
      vertx,
      aggregateClass,
      command
    );
  }

  private static void validateCommand(JsonObject command) {
    if (Objects.isNull(command.getString("command"))) {
      throw new CommandRejected(
        new EventxError(
          ErrorSource.LOGIC,
          "command",
          "command is null",
          "command must define command",
          "missing.param",
          400
        )
      );
    }
  }

  private static void validateClass(JsonObject command) {
    if (Objects.isNull(command.getString("commandClass"))) {
      throw new CommandRejected(
        new EventxError(
          ErrorSource.LOGIC,
          "command",
          "commandClass is null",
          "command must specify commandClass",
          "missing.param",
          400
        )
      );
    }
  }

  public Uni<Void> subscribe(Consumer<AggregateState<T>> consumer) {
    final var address = EventbusStateProjection.subscriptionAddress(aggregateClass, "default");
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
    final var address = EventbusStateProjection.subscriptionAddress(aggregateClass, tenantId);
    LOGGER.debug("Subscribing to state updates for {} in address {}", aggregateClass.getSimpleName(), address);
    return vertx.eventBus().<JsonObject>consumer(address).handler(jsonObjectMessage -> {
        LOGGER.debug("{} subscription incoming event {} {} ", aggregateClass.getSimpleName(), jsonObjectMessage.headers(), jsonObjectMessage.body());
        final var aggregateState = AggregateState.fromJson(jsonObjectMessage.body(), aggregateClass);
        consumer.accept(aggregateState);
      })
      .exceptionHandler(this::subscriptionError)
      .completionHandler();
  }

  public Uni<Void> eventSubscribe(Consumer<PolledEvent> consumer, String tenantId) {
    final var address = EventbusEventStream.eventbusAddress(aggregateClass, tenantId);
    LOGGER.debug("Subscribing to event stream for {} in address {}", aggregateClass.getSimpleName(), address);
    return vertx.eventBus().<JsonObject>consumer(address).handler(jsonObjectMessage -> {
        LOGGER.debug("{} subscription incoming event {} {} ", aggregateClass.getSimpleName(), jsonObjectMessage.headers(), jsonObjectMessage.body());
        final var aggregateState = jsonObjectMessage.body().mapTo(PolledEvent.class);
        consumer.accept(aggregateState);
      })
      .exceptionHandler(this::subscriptionError)
      .completionHandler();
  }

  public Uni<Void> eventSubscribe(Consumer<PolledEvent> consumer) {
    final var address = EventbusEventStream.eventbusAddress(aggregateClass, "default");
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
