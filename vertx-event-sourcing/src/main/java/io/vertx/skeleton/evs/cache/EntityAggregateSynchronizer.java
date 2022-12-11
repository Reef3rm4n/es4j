package io.vertx.skeleton.evs.cache;

import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.handlers.EntityAggregateConfiguration;
import io.vertx.skeleton.evs.objects.AggregateHandlerAction;
import io.vertx.skeleton.models.CacheException;
import io.vertx.skeleton.models.EntityAggregateKey;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.UniHelper;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.mutiny.core.shareddata.Counter;
import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.ServerNode;

import static io.vertx.skeleton.evs.handlers.EntityAggregateHandlerVerticle.ACTION;
import static io.vertx.skeleton.evs.handlers.EntityAggregateHandlerVerticle.CLASS_NAME;

public class EntityAggregateSynchronizer<T extends EntityAggregate> extends AbstractVerticle {
  private static final Logger LOGGER = LoggerFactory.getLogger(EntityAggregateSynchronizer.class);

  private final Class<T> aggregateClass;
  private EntityAggregateConfiguration entityAggregateConfiguration;

  ConsistentHash<ServerNode> ring = HashRing.<ServerNode>newBuilder()
    .name("proxyHashRing")       // set hash ring name
    .hasher(DefaultHasher.MURMUR_3)   // hash function to distribute partitions
    .partitionRate(10)                  // number of partitions per node
    .build();

  public EntityAggregateSynchronizer(final Class<T> aggregateClass) {
    this.aggregateClass = aggregateClass;
  }

  @Override
  public Uni<Void> asyncStart() {
    LOGGER.info("Starting " + this.getClass().getSimpleName() + " " + context.deploymentID());
    Uni<Void> syncUni = Uni.createFrom().voidItem();
    if (vertx.isClustered()) {
      syncUni = synchronizeNode().onFailure().recoverWithNull();
    }
    this.entityAggregateConfiguration = config().getJsonObject(aggregateClass.getSimpleName()).mapTo(EntityAggregateConfiguration.class);
    return syncUni.flatMap(avoid -> availableHandlersSynchronizer())
      .flatMap(avoid -> entityAggregateHandlerSynchronizer())
      .flatMap(avoid -> nodeSynchronizerService());
  }

  private String handlerAddress() {
    return this.aggregateClass.getName() + "." + context.deploymentID();
  }

  private Uni<Void> entityAggregateHandlerSynchronizer() {
    return vertx.eventBus().<JsonObject>consumer(AddressResolver.localAggregateHandler(aggregateClass))
      .handler(objectMessage -> {
          final var request = objectMessage.body().mapTo(EntityAggregateHandlerAddress.class);
          LOGGER.debug("Synchronizing EntityAggregate -> " + request);
          final var uni = switch (Actions.valueOf(objectMessage.headers().get(Actions.ACTION.name()))) {
            case ADD -> addEntityAggregateHandler(request);
            case REMOVE -> removeEntityAggregateHandler(request);
            default -> throw CacheException.illegalState();
          };
          uni.subscribe()
            .with(item -> LOGGER.debug("EntityAggregate synchronized -> " + request),
              throwable -> LOGGER.error("Unable to synchronize EntityAggregate -> " + request, throwable)
            );
        }
      )
      .exceptionHandler(this::handlerThrowable)
      .completionHandler();
  }

  private Uni<String> removeEntityAggregateHandler(EntityAggregateHandlerAddress handlerAddress) {
    return localHandlerCounterMap(handlerAddress.address())
      .flatMap(Counter::decrementAndGet)
      .map(avoid -> {
          if (localEntityAggregateHandlerMap().removeIfPresent(handlerAddress.aggregateKey(), handlerAddress.address())) {
            LOGGER.info("New EntityAggregate removed -> " + handlerAddress);
          }
          return handlerAddress.address();
        }
      );
  }

  private Uni<String> addEntityAggregateHandler(EntityAggregateHandlerAddress handlerAddress) {
    return localHandlerCounterMap(handlerAddress.address())
      .flatMap(Counter::incrementAndGet)
      .flatMap(avoid -> {
          if (entityAggregateConfiguration.replication()) {
            LOGGER.info("Requesting local handler " + handlerAddress() + " to load entity -> " + handlerAddress.aggregateKey());
            return vertx.eventBus().request(handlerAddress(), JsonObject.mapFrom(handlerAddress.aggregateKey()), new DeliveryOptions()
                .addHeader(ACTION, AggregateHandlerAction.LOAD.name())
                .setLocalOnly(true)
                .addHeader(CLASS_NAME, handlerAddress.aggregateKey().getClass().getName()))
              .replaceWith(handlerAddress.address());
          } else if (localEntityAggregateHandlerMap().putIfAbsent(handlerAddress.aggregateKey(), handlerAddress.address()) == null) {
            LOGGER.info("New EntityAggregate added -> " + handlerAddress);
          }
          return Uni.createFrom().item(handlerAddress.address());
        }
      );
  }

  private Uni<Void> synchronizeNode() {
    LOGGER.info("Starting node synchronization -> " + aggregateClass);
    return vertx.eventBus().<JsonArray>request(
        AddressResolver.synchronizerService(aggregateClass),
        "",
        new DeliveryOptions().addHeader(Actions.ACTION.name(), SynchronizeActions.HANDLERS.name())
      )
      .map(objectMessage -> {
          final var availableHandlers = objectMessage.body().stream().map(object -> JsonObject.mapFrom(object).mapTo(String.class)).toList();
          LOGGER.info("Node discovered the following available handlers -> " + availableHandlers);
          availableHandlers.forEach(this::addHandler);
          return Void.TYPE;
        }
      )
      .flatMap(avoid -> vertx.eventBus().<JsonArray>request(
            AddressResolver.synchronizerService(aggregateClass),
            "",
            new DeliveryOptions().addHeader(Actions.ACTION.name(), SynchronizeActions.AGGREGATES.name())
          )
          .flatMap(objectMessage -> {
              final var aggregateHandlers = objectMessage.body().stream()
                .map(object -> JsonObject.mapFrom(object).mapTo(EntityAggregateHandlerAddress.class))
                .toList();
              LOGGER.info("Node discovered the aggregate handlers -> " + aggregateHandlers);
              return Multi.createFrom().iterable(aggregateHandlers)
                .onItem().transformToUniAndConcatenate(this::addEntityAggregateHandler)
                .collect().asList()
                .replaceWithVoid();
            }
          )
      )
      .onFailure().invoke(this::handlerThrowable);
  }

  private Uni<Void> nodeSynchronizerService() {
    return vertx.eventBus().consumer(AddressResolver.synchronizerService(aggregateClass))
      .handler(objectMessage -> {
          LOGGER.info("Node received a synchronization request");
          final var action = SynchronizeActions.valueOf(objectMessage.headers().get(Actions.ACTION.name()));
          if (action == SynchronizeActions.HANDLERS) {
            final var keys = localAvailableHandlersMap().keySet();
            LOGGER.info("Node forwarding the following available handlers -> " + keys);
            objectMessage.reply(JsonObject.mapFrom(new HandlerList(keys)));
          } else {
            final var handlerAddresses = vertx.getDelegate().sharedData().<EntityAggregateKey, String>getLocalMap(AddressResolver.localAggregateHandler(aggregateClass))
              .entrySet().stream()
              .map(entry -> new EntityAggregateHandlerAddress(entry.getKey(), entry.getValue()))
              .toList();
            LOGGER.info("Node forwarding the EntityAggregate handlers -> " + handlerAddresses);
            final var response = new JsonArray(handlerAddresses);
            objectMessage.reply(response);
          }
        }
      )
      .exceptionHandler(this::handlerThrowable)
      .completionHandler();
  }

  private Uni<Void> availableHandlersSynchronizer() {
    return vertx.eventBus().<String>consumer(AddressResolver.localAvailableHandlers(aggregateClass))
      .handler(objectMessage -> {
          LOGGER.debug("Synchronizing handler -> " + objectMessage.body());
          final var uni = switch (Actions.valueOf(objectMessage.headers().get(Actions.ACTION.name()))) {
            case ADD -> Uni.createFrom().item(addHandler(objectMessage.body())).replaceWithVoid();
            case REMOVE -> removeHandler(objectMessage.body());
            default -> throw CacheException.illegalState();
          };
          uni.subscribe()
            .with(UniHelper.NOOP,
              throwable -> LOGGER.error("Unable to update available handlers", throwable)
            );
        }
      )
      .exceptionHandler(this::handlerThrowable)
      .completionHandler();
  }

  private LocalMap<String, String> localAvailableHandlersMap() {
    return vertx.getDelegate().sharedData().<String, String>getLocalMap(AddressResolver.localAvailableHandlers(aggregateClass));
  }

  private io.vertx.mutiny.core.shareddata.LocalMap<EntityAggregateKey, String> localEntityAggregateHandlerMap() {
    return vertx.sharedData().<EntityAggregateKey, String>getLocalMap(AddressResolver.localAggregateHandler(aggregateClass));
  }

  private Uni<Counter> localHandlerCounterMap(final String handlerAddress) {
    return vertx.sharedData().getLocalCounter(AddressResolver.localAvailableHandlerCounter(aggregateClass, handlerAddress));
  }

  private String addHandler(final String handler) {
    if (localAvailableHandlersMap().get(handler) == null) {
      localAvailableHandlersMap().put(handler, "");
      LOGGER.info("New Handler added -> " + handler);
    }
    return handler;
  }

  private Uni<Void> removeHandler(String handler) {
    return localHandlerCounterMap(handler)
      .flatMap(counter -> counter.get()
        .flatMap(size -> counter.addAndGet(size * - 1L))
      )
      .map(avoid -> {
          localAvailableHandlersMap().remove(handler);
          LOGGER.info("Handler removed -> " + handler);
          return handler;
        }
      )
      .replaceWithVoid();
  }

  private void handlerThrowable(final Throwable throwable) {
    LOGGER.error("[-- EntityAggregateSynchronizer had to drop the following exception --]", throwable);
  }

  @Override
  public Uni<Void> asyncStop() {
    LOGGER.info("Stopping " + this.getClass().getSimpleName() + " deploymentID -> " + this.deploymentID());
    return super.asyncStop();
  }
}
