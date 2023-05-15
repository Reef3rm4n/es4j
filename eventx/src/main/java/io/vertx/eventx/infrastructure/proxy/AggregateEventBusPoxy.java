package io.vertx.eventx.infrastructure.proxy;

import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Command;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.core.EventbusEventProjection;
import io.vertx.eventx.core.EventbusStateProjection;
import io.vertx.eventx.objects.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.exceptions.CommandRejected;
import io.vertx.mutiny.core.Vertx;

import java.util.function.Consumer;

import static io.vertx.eventx.infrastructure.bus.AggregateBus.request;


public class AggregateEventBusPoxy<T extends Aggregate> {
  private final Vertx vertx;
  public final Class<T> aggregateClass;
  public static final Logger LOGGER = LoggerFactory.getLogger(AggregateEventBusPoxy.class);

  public AggregateEventBusPoxy(
    final Vertx vertx,
    final Class<T> entityClass
  ) {
    this.vertx = vertx;
    this.aggregateClass = entityClass;
  }

  public Uni<AggregateState<T>> load(LoadAggregate loadCommand) {
    ContextualData.put("TENANT", loadCommand.headers().tenantId());
    return request(
      vertx,
      aggregateClass,
      JsonObject.mapFrom(new CommandWrapper(loadCommand.getClass().getName(), loadCommand)),
      Action.LOAD
    );
  }


  public Uni<AggregateState<T>> command(Command command) {
    ContextualData.put("TENANT", command.headers().tenantId());
    return request(
      vertx,
      aggregateClass,
      JsonObject.mapFrom(new CommandWrapper(command.getClass().getName(), command)),
      Action.COMMAND
    );
  }

  public record CommandWrapper(
    String commandClass,
    Command command
  ) {
  }

  public Uni<AggregateState<T>> command(final JsonObject command) {
    if (command.getString("commandClass") == null) {
      throw new CommandRejected(
        new EventxError(
          ErrorSource.LOGIC,
          "command",
          "commandClass is null",
          "command must have both commandClass and command",
          "missing.param",
          400
        )
      );
    }
    return request(
      vertx,
      aggregateClass,
      command,
      Action.COMMAND
    );
  }


  public Uni<Void> subscribe(Consumer<AggregateState<T>> consumer) {
    final var address = EventbusStateProjection.subscriptionAddress(aggregateClass);
    LOGGER.debug("Subscribing to state updates for {} in address {}", aggregateClass.getSimpleName(), address);
    return vertx.eventBus().<JsonObject>consumer(
        EventbusStateProjection.subscriptionAddress(aggregateClass))
      .handler(jsonObjectMessage -> {
        LOGGER.debug("{} subscription incoming event {} {} ", aggregateClass.getSimpleName(), jsonObjectMessage.headers(), jsonObjectMessage.body());
        final var aggregateState = AggregateState.fromJson(jsonObjectMessage.body(), aggregateClass);
        consumer.accept(aggregateState);
      })
      .exceptionHandler(this::subscriptionError)
      .completionHandler();
  }

  public Uni<Void> eventSubscribe(Consumer<PolledEvent> consumer) {
    final var address = EventbusEventProjection.eventbusAddress(aggregateClass);
    LOGGER.debug("Subscribing to event stream for {} in address {}", aggregateClass.getSimpleName(), address);
    return vertx.eventBus().<JsonObject>consumer(
        EventbusEventProjection.eventbusAddress(aggregateClass))
      .handler(jsonObjectMessage -> {
        LOGGER.debug("{} subscription incoming event {} {} ", aggregateClass.getSimpleName(), jsonObjectMessage.headers(), jsonObjectMessage.body());
        final var aggregateState = jsonObjectMessage.body().mapTo(PolledEvent.class);
        consumer.accept(aggregateState);
      })
      .exceptionHandler(this::subscriptionError)
      .completionHandler();
  }

  private void subscriptionError(Throwable throwable) {
    LOGGER.error("{} subscription dropped exception", aggregateClass, throwable);
  }


}
