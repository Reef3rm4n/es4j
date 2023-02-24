package io.vertx.skeleton.evs.actors;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.FixedDemandPacer;
import io.vertx.mutiny.core.eventbus.Message;
import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.evs.Entity;
import io.vertx.skeleton.evs.cache.Actions;
import io.vertx.skeleton.evs.cache.AddressResolver;
import io.vertx.skeleton.evs.consistenthashing.exceptions.EmptyHashRingException;
import io.vertx.skeleton.evs.exceptions.NodeNotFoundException;
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
import io.vertx.skeleton.models.exceptions.CacheException;
import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.time.Duration;

import static io.vertx.core.eventbus.ReplyFailure.RECIPIENT_FAILURE;

// todo add circuit breakers
public class ChannelProxy<T extends Entity> {
  public static final String APPLICATION_JSON = "application/json";
  public static final String CONTENT_TYPE = "content-type";
  public static final String ACTION = "action";
  public static final String CLASS_NAME = "className";
  private final Vertx vertx;
  public final Class<T> aggregateEntityClass;
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelProxy.class);

  private final ConsistentHash<SimpleNode> hashRing;

  public ChannelProxy(
    final Vertx vertx,
    final Class<T> aggregateEntityClass
  ) {
    this.vertx = vertx;
    this.aggregateEntityClass = aggregateEntityClass;
    this.hashRing = HashRing.<SimpleNode>newBuilder()
      .name(aggregateEntityClass.getSimpleName() + "entity-aggregate-hash-ring")       // set hash ring name
      .hasher(DefaultHasher.MURMUR_3)   // hash function to distribute partitions
      .partitionRate(100000)                  // number of partitions per node
      .build();

  }

  public Uni<T> load(String entityId, CommandHeaders requestMetadata) {
    final var entityKey = new EntityAggregateKey(entityId, requestMetadata.tenantId());
    final var handlerAddress = locateHandler(entityKey);
    return vertx.eventBus().<JsonObject>request(
        handlerAddress,
        JsonObject.mapFrom(entityKey),
        new DeliveryOptions()
          .addHeader(ACTION, AggregateHandlerAction.LOAD.name())
      )
      .map(response -> response.body().mapTo(aggregateEntityClass))
      .onFailure().transform(Unchecked.function(ChannelProxy::transformError));
  }

  public <C extends Command> Uni<T> forwardCommand(C command) {
    final var commandWrapper = new CommandWrapper(command.entityId(), new io.vertx.skeleton.evs.objects.Command(command.getClass().getName(), JsonObject.mapFrom(command)), command.requestHeaders());
    return forwardCommand(commandWrapper);
  }

  public Uni<T> forwardCommand(final CommandWrapper command) {
    final var handlerAddress = locateHandler(new EntityAggregateKey(command.entityId(), command.commandHeaders().tenantId()));
    return vertx.eventBus().<JsonObject>request(
        handlerAddress,
        JsonObject.mapFrom(command),
        new DeliveryOptions()
          .addHeader(ACTION, AggregateHandlerAction.COMMAND.name())
          .addHeader(CLASS_NAME, command.command().commandType())
      )
      .map(response -> response.body().mapTo(aggregateEntityClass))
      .onFailure().transform(Unchecked.function(ChannelProxy::transformError));
  }

  public Uni<T> handleCompositeCommand(final CompositeCommandWrapper compositeCmd) {
    final var handlerAddress = locateHandler(new EntityAggregateKey(compositeCmd.entityId(), compositeCmd.commandHeaders().tenantId()));
    return vertx.eventBus().<JsonObject>request(
        handlerAddress,
        JsonObject.mapFrom(compositeCmd),
        new DeliveryOptions()
          .addHeader(ACTION, AggregateHandlerAction.COMPOSITE_COMMAND.name())
          .addHeader(CLASS_NAME, compositeCmd.getClass().getName())
      )
      .map(response -> response.body().mapTo(aggregateEntityClass))
      .onFailure().transform(Unchecked.function(ChannelProxy::transformError));
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
