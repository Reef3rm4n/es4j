package io.vertx.eventx.infrastructure.proxies;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Command;
import io.vertx.eventx.Aggregate;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.infrastructure.models.AggregatePlainKey;
import io.vertx.eventx.objects.Action;
import io.vertx.mutiny.core.Vertx;

import static io.vertx.eventx.infrastructure.bus.AggregateBus.request;


public class AggregateEventBusClient<T extends Aggregate> {
  private final Vertx vertx;
  public final Class<T> aggregateClass;
  public static final Logger LOGGER = LoggerFactory.getLogger(AggregateEventBusClient.class);

  public AggregateEventBusClient(
    final Vertx vertx,
    final Class<T> entityClass
  ) {
    this.vertx = vertx;
    this.aggregateClass = entityClass;
  }

  public Uni<T> wakeUp(AggregatePlainKey key) {
    return request(
      vertx,
      aggregateClass,
      JsonObject.mapFrom(key),
      Action.LOAD
    );
  }


  public Uni<T> command(Command command) {
    return request(
      vertx,
      aggregateClass,
      JsonObject.mapFrom(command),
      Action.COMMAND
    );
  }

  public Uni<T> command(final JsonObject command) {
    return request(
      vertx,
      aggregateClass,
      command,
      Action.COMMAND
    );
  }


}
