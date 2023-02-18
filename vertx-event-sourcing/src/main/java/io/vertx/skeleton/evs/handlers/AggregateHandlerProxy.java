package io.vertx.skeleton.evs.handlers;

import io.smallrye.mutiny.vertx.UniHelper;
import io.vertx.mutiny.core.eventbus.Message;
import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.cache.Actions;
import io.vertx.skeleton.evs.cache.AddressResolver;
import io.vertx.skeleton.evs.exceptions.HandlerNotFoundException;
import io.vertx.skeleton.evs.exceptions.RejectedCommandException;
import io.vertx.skeleton.evs.objects.*;
import io.vertx.skeleton.models.*;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.RoutingContext;
import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.CacheException;
import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import static io.vertx.core.eventbus.ReplyFailure.RECIPIENT_FAILURE;

// todo add circuit breakers
public class AggregateHandlerProxy<T extends EntityAggregate> {
  public static final String APPLICATION_JSON = "application/json";
  public static final String CONTENT_TYPE = "content-type";
  public static final String ACTION = "action";
  public static final String CLASS_NAME = "className";
  private final Vertx vertx;
  private final Class<T> aggregateEntityClass;
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateHandlerProxy.class);

  private final ConsistentHash<SimpleNode> hashRing;

  public AggregateHandlerProxy(
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
    hashRingSynchronizer();
  }


  public void load(String entityId, RequestMetadata requestMetadata, RoutingContext routingContext) {
    final var entityKey = new EntityAggregateKey(entityId, requestMetadata.tenant());
    final var handlerAddress = locateHandler(entityKey);
    vertx.eventBus().<JsonObject>request(
        handlerAddress,
        JsonObject.mapFrom(entityKey),
        new DeliveryOptions()
          .addHeader(ACTION, AggregateHandlerAction.LOAD.name())
      )
      .map(response -> response.body().mapTo(aggregateEntityClass))
      .onFailure().transform(Unchecked.function(AggregateHandlerProxy::transformError))
      .subscribe()
      .with(
        response -> ok(routingContext, response),
        routingContext::fail
      );
  }

  public void forwardCommand(final CommandWrapper command, RoutingContext routingContext) {
    final var handlerAddress = locateHandler(new EntityAggregateKey(command.entityId(), command.requestMetadata().tenant()));
    vertx.eventBus().<JsonObject>request(
        handlerAddress,
        JsonObject.mapFrom(command),
        new DeliveryOptions()
          .addHeader(ACTION, AggregateHandlerAction.COMMAND.name())
          .addHeader(CLASS_NAME, command.command().commandType())
      )
      .map(response -> response.body().mapTo(aggregateEntityClass))
      .onFailure().transform(Unchecked.function(AggregateHandlerProxy::transformError))
      .subscribe()
      .with(
        response -> ok(routingContext, response),
        routingContext::fail
      );
  }

  public void handleCompositeCommand(final CompositeCommandWrapper compositeCmd, RoutingContext routingContext) {
    final var handlerAddress = locateHandler(new EntityAggregateKey(compositeCmd.entityId(), compositeCmd.requestMetadata().tenant()));
    vertx.eventBus().<JsonObject>request(
        handlerAddress,
        JsonObject.mapFrom(compositeCmd),
        new DeliveryOptions()
          .addHeader(ACTION, AggregateHandlerAction.COMPOSITE_COMMAND.name())
          .addHeader(CLASS_NAME, compositeCmd.getClass().getName())
      )
      .map(response -> response.body().mapTo(aggregateEntityClass))
      .onFailure().transform(Unchecked.function(AggregateHandlerProxy::transformError))
      .subscribe()
      .with(
        response -> ok(routingContext, response),
        routingContext::fail
      );
  }

  private static Throwable transformError(final Throwable throwable) {
    if (throwable instanceof ReplyException replyException) {
      LOGGER.error("ReplyException from handler -> ", replyException);
      if (replyException.failureType() == RECIPIENT_FAILURE) {
        try {
          final var error = new JsonObject(replyException.getLocalizedMessage()).mapTo(Error.class);
          return new RejectedCommandException(error);
        } catch (IllegalArgumentException illegalArgumentException) {
          LOGGER.error("Unable to parse rejectCommandException -> ", illegalArgumentException);
          return new RejectedCommandException(new Error(throwable.getMessage(), null, 500));
        }
      } else {
        return new RejectedCommandException(new Error(replyException.failureType().name(), replyException.getMessage(), replyException.failureCode()));
      }
    } else {
      LOGGER.error("Unknown exception from handler -> ", throwable);
      return new RejectedCommandException(new Error(throwable.getMessage(), null, 500));
    }
  }

  private void ok(RoutingContext routingContext, Object o) {
    routingContext.response().setStatusCode(200)
      .putHeader(CONTENT_TYPE, APPLICATION_JSON)
      .send(JsonObject.mapFrom(o).encode())
      .subscribe()
      .with(
        UniHelper.NOOP,
        // todo add more info to error log, stuff like entityId and request
        throwable -> LOGGER.error("Unable to reply to caller", throwable)
      );
  }


  private void addHandler(final String handler) {
    final var simpleNode = SimpleNode.of(handler);
    if (!hashRing.contains(simpleNode)) {
      LOGGER.info("Adding new handler to hash-ring -> " + handler);
      hashRing.add(simpleNode);
    } else {
      LOGGER.info("Handler already present in hash-ring -> " + handler);
    }
  }

  public String locateHandler(EntityAggregateKey key) {
    LOGGER.info("Looking for handler for entity -> " + key);
    final var node = hashRing.locate(key.entityId()).orElseThrow(() -> new HandlerNotFoundException(key.entityId()));
    LOGGER.info("Resolved to node -> " + node);
    return node.getKey();
  }

  private void hashRingSynchronizer() {
    //todo
    // - create an interface for this synchronization
    // - create an implementation using vert.x event-bus
    // - create an implementation using  pg pub/sub
    vertx.eventBus().<String>consumer(AddressResolver.localAvailableHandlers(aggregateEntityClass))
      .handler(this::synchronizeHashRing)
      .exceptionHandler(this::handlerThrowable)
      .completionHandler()
      .subscribe()
      .with(
        avoid -> LOGGER.info("Hash ring synchronizer deployed"),
        throwable -> LOGGER.error("Unable to deploy " + aggregateEntityClass + " proxy", throwable)
      );
  }

  private void synchronizeHashRing(Message<String> objectMessage) {
    LOGGER.debug("Synchronizing handler -> " + objectMessage.body());
    switch (Actions.valueOf(objectMessage.headers().get(Actions.ACTION.name()))) {
      case ADD -> addHandler(objectMessage.body());
      case REMOVE -> removeHandler(objectMessage.body());
      default -> throw CacheException.illegalState();
    }
  }

  private void removeHandler(final String handler) {
    final var simpleNode = SimpleNode.of(handler);
    if (hashRing.contains(simpleNode)) {
      LOGGER.info("Removing handler form hash ring -> " + handler);
      hashRing.remove(simpleNode);
    } else {
      LOGGER.info("Handler not present in hash ring -> " + handler);
    }
  }

  private void handlerThrowable(final Throwable throwable) {
    LOGGER.error("[-- EntityAggregateSynchronizer for entity " + aggregateEntityClass.getSimpleName() + " had to drop the following exception --]", throwable);
  }

}
