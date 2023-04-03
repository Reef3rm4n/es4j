package io.vertx.eventx.infrastructure.proxies;

import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Command;
import io.vertx.eventx.Aggregate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.exceptions.CommandRejected;
import io.vertx.eventx.infrastructure.models.AggregatePlainKey;
import io.vertx.eventx.objects.Action;
import io.vertx.eventx.objects.AggregateState;
import io.vertx.eventx.objects.ErrorSource;
import io.vertx.eventx.objects.EventxError;
import io.vertx.mutiny.core.Vertx;

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

  public Uni<AggregateState<T>> wakeUp(AggregatePlainKey key) {
    ContextualData.put("TENANT", key.tenantId());
    ContextualData.put("COMMAND", "wakeUp");
    return request(
      vertx,
      aggregateClass,
      JsonObject.mapFrom(key),
      Action.LOAD
    );
  }


  public Uni<AggregateState<T>> command(Command command) {
    ContextualData.put("TENANT", command.headers().tenantId());
    ContextualData.put("COMMAND", command.getClass().getSimpleName());
    return request(
      vertx,
      aggregateClass,
      JsonObject.mapFrom(new CommandWrapper(command.getClass().getName(),command)),
      Action.COMMAND
    );
  }

  public record CommandWrapper(
    String commandClass,
    Command command
  ){}

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


}
