package io.vertx.skeleton.evs.actors;

import io.smallrye.mutiny.Uni;
import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.evs.Entity;
import io.vertx.skeleton.evs.exceptions.CommandRejected;
import io.vertx.skeleton.evs.objects.*;
import io.vertx.skeleton.models.*;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.skeleton.models.Error;

import static io.vertx.core.eventbus.ReplyFailure.RECIPIENT_FAILURE;

// todo add circuit breakers
public class ChannelProxy<T extends Entity> {
  public static final String APPLICATION_JSON = "application/json";
  public static final String CONTENT_TYPE = "content-type";
  public static final String ACTION = "action";
  public static final String CLASS_NAME = "className";
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

  public Uni<T> load(String entityId, CommandHeaders requestMetadata) {
    final var entityKey = new EntityKey(entityId, requestMetadata.tenantId());
    return request(entityKey, JsonObject.mapFrom(entityKey), ActorCommand.LOAD);
  }

  private Uni<T> request(EntityKey entityKey, JsonObject payload, ActorCommand actorCommand) {
    return vertx.eventBus().<JsonObject>request(
        Channel.resolveActor(entityClass, entityKey),
        payload,
        new DeliveryOptions()
          .addHeader(ACTION, actorCommand.name())
      )
      .onFailure(ReplyException.class).retry().atMost(3)
      .map(response -> response.body().mapTo(entityClass))
      .onFailure().transform(Unchecked.function(ChannelProxy::transformError));
  }


  public <C extends Command> Uni<T> forwardCommand(C command) {
    final var commandWrapper = new CommandWrapper(command.entityId(), new io.vertx.skeleton.evs.objects.Command(command.getClass().getName(), JsonObject.mapFrom(command)), command.commandHeaders());
    return request(
      new EntityKey(command.entityId(), command.commandHeaders().tenantId()),
      JsonObject.mapFrom(commandWrapper),
      ActorCommand.COMMAND
    );
  }

  public Uni<T> forwardCommand(final CommandWrapper command) {
    return request(
      new EntityKey(command.entityId(), command.commandHeaders().tenantId()),
      JsonObject.mapFrom(command),
      ActorCommand.COMMAND
    );
  }

  public Uni<T> handleCompositeCommand(final CompositeCommandWrapper compositeCmd) {
    return request(
      new EntityKey(compositeCmd.entityId(), compositeCmd.commandHeaders().tenantId()),
      JsonObject.mapFrom(compositeCmd),
      ActorCommand.COMPOSITE_COMMAND
    );
  }

  private static Throwable transformError(final Throwable throwable) {
    if (throwable instanceof ReplyException replyException) {
      LOGGER.error("ReplyException from handler -> ", replyException);
      if (replyException.failureType() == RECIPIENT_FAILURE) {
        try {
          final var error = new JsonObject(replyException.getLocalizedMessage()).mapTo(Error.class);
          return new CommandRejected(error);
        } catch (IllegalArgumentException illegalArgumentException) {
          LOGGER.error("Unable to parse rejectCommandException -> ", illegalArgumentException);
          return new CommandRejected(new Error(throwable.getMessage(), null, 500));
        }
      } else {
        return new CommandRejected(new Error(replyException.failureType().name(), replyException.getMessage(), replyException.failureCode()));
      }
    } else {
      LOGGER.error("Unknown exception from handler -> ", throwable);
      return new CommandRejected(new Error(throwable.getMessage(), null, 500));
    }
  }


}
