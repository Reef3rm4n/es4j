package io.vertx.skeleton.evs.handlers;

import io.vertx.skeleton.evs.EntityAggregateQuery;
import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.cache.Actions;
import io.vertx.skeleton.evs.cache.AddressResolver;
import io.vertx.skeleton.evs.cache.EntityAggregateProxyCache;
import io.vertx.skeleton.evs.objects.AggregateHandlerAction;
import io.vertx.skeleton.evs.EntityAggregateCommand;
import io.vertx.skeleton.evs.objects.CompositeCommand;
import io.vertx.skeleton.models.CacheException;
import io.vertx.skeleton.models.EntityAggregateKey;
import io.vertx.skeleton.models.Error;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.RoutingContext;
import io.vertx.skeleton.models.RejectedCommandException;
import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.util.Optional;

import static io.vertx.core.eventbus.ReplyFailure.RECIPIENT_FAILURE;

// todo add distributed circuit breakers
public class AggregateHandlerProxy<T extends EntityAggregate> {
  public static final String APPLICATION_JSON = "application/json";
  public static final String CONTENT_TYPE = "content-type";
  public static final String ACTION = "action";
  public static final String CLASS_NAME = "className";
  private final Vertx vertx;
  private final EntityAggregateProxyCache<T> aggregateHandlerCache;
  private final Class<T> aggregateEntityClass;
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateHandlerProxy.class);

  private final static ConsistentHash<SimpleNode> ring = HashRing.<SimpleNode>newBuilder()
    .name("proxyHashRing")       // set hash ring name
    .hasher(DefaultHasher.MURMUR_3)   // hash function to distribute partitions
    .partitionRate(100000)                  // number of partitions per node
    .build();

  public AggregateHandlerProxy(
    final Vertx vertx,
    final Class<T> aggregateEntityClass
  ) {
    this.vertx = vertx;
    this.aggregateEntityClass = aggregateEntityClass;
    this.aggregateHandlerCache = new EntityAggregateProxyCache<>(
      vertx,
      aggregateEntityClass
    );
  }


  public <Q extends EntityAggregateQuery, R> void handleQuery(final Q query, final Class<R> responseType, RoutingContext routingContext) {
    forwardQuery(query,responseType)
      .subscribe()
      .with(
        response -> ok(routingContext, response),
        routingContext::fail
      );
  }

  public <Q extends EntityAggregateQuery, R> Uni<R> forwardQuery(final Q query, final Class<R> responseType) {
    return aggregateHandlerCache.getHandler(new EntityAggregateKey(query.entityId(), query.requestMetadata().tenant()))
      .flatMap(handlerAddress -> vertx.eventBus().<JsonObject>request(
          handlerAddress,
          JsonObject.mapFrom(query),
          new DeliveryOptions()
            .addHeader(ACTION, AggregateHandlerAction.QUERY.name())
            .addHeader(CLASS_NAME, query.getClass().getName())
        )
      )
      .map(response -> response.body().mapTo(responseType))
      .onFailure().transform(Unchecked.function(AggregateHandlerProxy::transformError));
  }

//  public <Q extends EntityAggregateQuery, R> Uni<R> forwardProjectionQuery(final Q query, final Class<R> responseType) {
//    return aggregateHandlerCache.getHandler(new EntityAggregateKey(query.entityId(), query.requestMetadata().tenant()))
//      .flatMap(handlerAddress -> vertx.eventBus().<JsonObject>request(
//          handlerAddress,
//          JsonObject.mapFrom(query),
//          new DeliveryOptions()
//            .addHeader(ACTION, AggregateHandlerAction.QUERY_PROJECTION.name())
//            .addHeader(CLASS_NAME, query.getClass().getName())
//        )
//      )
//      .map(response -> response.body().mapTo(responseType))
//      .onFailure().transform(Unchecked.function(AggregateHandlerProxy::transformError));
//  }

//  public <Q extends EntityAggregateQuery, R> void handleProjectionQuery(final Q query, final Class<R> responseType, RoutingContext routingContext) {
//    forwardProjectionQuery(query, responseType)
//      .subscribe()
//      .with(
//        response -> ok(routingContext, response),
//        routingContext::fail
//      );
//  }

  public <C extends EntityAggregateCommand> Uni<T> forwardCommand(final C command) {
    return aggregateHandlerCache.getHandler(new EntityAggregateKey(command.entityId(), command.requestMetadata().tenant()))
      .flatMap(handlerAddress -> vertx.eventBus().<JsonObject>request(
          handlerAddress,
          JsonObject.mapFrom(command),
          new DeliveryOptions()
            .addHeader(ACTION, AggregateHandlerAction.COMMAND.name())
            .addHeader(CLASS_NAME, command.getClass().getName())
        )
      )
      .map(response -> response.body().mapTo(aggregateEntityClass))
      .onFailure().transform(Unchecked.function(AggregateHandlerProxy::transformError));
  }

  public <C extends EntityAggregateCommand> void handleCommand(final C command, RoutingContext routingContext) {
    forwardCommand(command)
      .subscribe()
      .with(
        response -> ok(routingContext, response),
        routingContext::fail
      );
  }

  public void handleCompositeCommand(final CompositeCommand compositeCommand, RoutingContext routingContext) {
    aggregateHandlerCache.getHandler(new EntityAggregateKey(compositeCommand.entityId(), compositeCommand.requestMetadata().tenant()))
      .flatMap(handlerAddress -> vertx.eventBus().<JsonObject>request(
          handlerAddress,
          JsonObject.mapFrom(compositeCommand),
          new DeliveryOptions()
            .addHeader(ACTION, AggregateHandlerAction.COMPOSITE_COMMAND.name())
            .addHeader(CLASS_NAME, compositeCommand.getClass().getName())
        )
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
//      LOGGER.error("Parsing exception -> ", replyException);
      if (replyException.failureType() == RECIPIENT_FAILURE) {
        try {
          final var error = new JsonObject(replyException.getLocalizedMessage()).mapTo(Error.class);
          return new RejectedCommandException(error);
        } catch (IllegalArgumentException illegalArgumentException) {
          LOGGER.error("Error parsing Error json -> ", illegalArgumentException);
          return new RejectedCommandException(new Error(throwable.getMessage(), null, 500));
        }
      } else {
        return new RejectedCommandException(new Error(replyException.failureType().name(), replyException.getMessage(), replyException.failureCode()));
      }
    } else {
      LOGGER.error("Unknown exception exception -> ", throwable);
      return new RejectedCommandException(new Error(throwable.getMessage(), null, 500));
    }
  }

  private void ok(RoutingContext routingContext, Object o) {
    routingContext.response().setStatusCode(200)
      .putHeader(CONTENT_TYPE, APPLICATION_JSON)
      .sendAndForget(JsonObject.mapFrom(o).encode());
  }

  private Optional<String> locateHandler(EntityAggregateKey key) {
     return ring.locate(key.entityId() + "::" + key.tenant().generateString()).map(SimpleNode::getKey);
  }

  private void addHandler(final String handler) {
    final var simpleNode = SimpleNode.of(handler);
    if (!ring.contains(simpleNode)) {
      LOGGER.info("Adding new handler to hash ring -> " + handler);
      ring.add(simpleNode);
    } else {
      LOGGER.info("Handler already present in hash ring -> " + handler);
    }
  }

  private Uni<Void> availableHandlersSynchronizer() {
    return vertx.eventBus().<String>consumer(AddressResolver.localAvailableHandlers(aggregateEntityClass))
      .handler(objectMessage -> {
          LOGGER.debug("Synchronizing handler -> " + objectMessage.body());
          switch (Actions.valueOf(objectMessage.headers().get(Actions.ACTION.name()))) {
            case ADD -> addHandler(objectMessage.body());
            case REMOVE -> removeHandler(objectMessage.body());
            default -> throw CacheException.illegalState();
          }
        }
      )
      .exceptionHandler(this::handlerThrowable)
      .completionHandler();
  }

  private void removeHandler(final String handler) {
    final var simpleNode = SimpleNode.of(handler);
    if (ring.contains(simpleNode)) {
      LOGGER.info("Removing handler form hash ring -> " + handler);
      ring.remove(simpleNode);
    } else {
      LOGGER.info("Handler not present in hash ring -> " + handler);
    }
  }

  private void handlerThrowable(final Throwable throwable) {
    LOGGER.error("[-- EntityAggregateSynchronizer had to drop the following exception --]", throwable);
  }

}
