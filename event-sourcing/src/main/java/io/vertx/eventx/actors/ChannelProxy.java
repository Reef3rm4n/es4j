package io.vertx.eventx.actors;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Command;
import io.vertx.eventx.Aggregate;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.objects.Action;
import io.vertx.eventx.storage.pg.models.AggregateKey;
import io.vertx.mutiny.core.Vertx;

import static io.vertx.eventx.actors.Channel.request;


public class ChannelProxy<T extends Aggregate> {
  private final Vertx vertx;
  public final Class<T> entityClass;
  public static final Logger LOGGER = LoggerFactory.getLogger(ChannelProxy.class);

  public ChannelProxy(
    final Vertx vertx,
    final Class<T> entityClass
  ) {
    this.vertx = vertx;
    this.entityClass = entityClass;
  }

  public Uni<T> wakeUp(AggregateKey key) {
    return request(
      vertx,
      entityClass,
      JsonObject.mapFrom(key),
      Action.LOAD
    );
  }


  public Uni<T> command(Command command) {
    return request(
      vertx,
      entityClass,
      JsonObject.mapFrom(command),
      Action.COMMAND
    );
  }

  public Uni<T> command(final JsonObject command) {
    return request(
      vertx,
      entityClass,
      command,
      Action.COMMAND
    );
  }

//  public Uni<T> compositeCommand(final List<Command> command) {
//    return request(
//      vertx,
//      entityClass,
//      new JsonArray(command),
//      Action.COMPOSITE_COMMAND
//    );
//  }


}
