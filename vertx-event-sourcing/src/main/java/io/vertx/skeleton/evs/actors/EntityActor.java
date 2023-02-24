package io.vertx.skeleton.evs.actors;

import io.activej.inject.Injector;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.core.Vertx;
import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.evs.Behaviour;
import io.vertx.skeleton.evs.Entity;
import io.vertx.skeleton.evs.Aggregator;
import io.vertx.skeleton.evs.cache.EntityAggregateCache;
import io.vertx.skeleton.evs.objects.*;
import io.vertx.skeleton.models.exceptions.VertxServiceException;
import io.vertx.skeleton.sql.Repository;
import io.vertx.skeleton.sql.RepositoryHandler;
import io.vertx.skeleton.evs.mappers.AggregateSnapshotMapper;
import io.vertx.skeleton.evs.mappers.EventJournalMapper;
import io.vertx.skeleton.evs.mappers.RejectedCommandMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.utils.CustomClassLoader;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

// todo move cache to caffeine for better performance
// ensure that it always gets executed in the current context
//    Caffeine.newBuilder()
//        .expireAfterWrite(Duration.ofMinutes(entityAggregateConfiguration.aggregateCacheTtlInMinutes()))
//        .recordStats() // (3)
//        .executor(cmd -> context.runOnContext(v -> cmd.run())) // (4)
//        .buildAsync((key, exec) -> CompletableFuture.supplyAsync(() -> { // (5)
//          Future<Buffer> future = fetchCatImage(key); // (6)
//          return future.toCompletionStage(); // (7)
//        }, exec).thenComposeAsync(Function.identity(), exec));
public class EntityActor<T extends Entity> extends AbstractVerticle {

  protected static final Logger LOGGER = LoggerFactory.getLogger(EntityActor.class);

  public static final String ACTION = "action";
  public static final String CLASS_NAME = "className";
  private List<CommandBehaviourWrapper> commandBehaviours;
  private List<EventBehaviourWrapper> eventBehaviours;
  private final ModuleBuilder moduleBuilder;
  private EntityAggregateCache<T, EntityAggregateState<T>> entityAggregateCache = null;
  private Channel<T> Channel;
  private final Class<T> entityAggregateClass;
  private EntityAggregateConfiguration entityAggregateConfiguration;
  private RepositoryHandler repositoryHandler;
  public ActorLogic<T> ActorLogic;

  public EntityActor(
    final Class<T> entityAggregateClass,
    final ModuleBuilder moduleBuilder
  ) {
    this.entityAggregateClass = entityAggregateClass;
    this.moduleBuilder = moduleBuilder;
  }

  @Override
  public Uni<Void> asyncStart() {
    this.entityAggregateConfiguration = config().getJsonObject("entityAggregateConfiguration", new JsonObject()).mapTo(EntityAggregateConfiguration.class);
    LOGGER.info("Starting " + this.getClass().getSimpleName() + " " + context.deploymentID() + " configuration -> " + JsonObject.mapFrom(entityAggregateConfiguration).encodePrettily());
    final var injector = startInjector();
    this.eventBehaviours = loadEventBehaviours(injector, entityAggregateClass);
    this.commandBehaviours = loadCommandBehaviours(injector, entityAggregateClass);
    this.Channel = new Channel<>(vertx, entityAggregateConfiguration.handlerHeartBeatInterval(), entityAggregateClass, handlerAddress());
    if (Boolean.TRUE.equals(entityAggregateConfiguration.useCache())) {

      this.entityAggregateCache = new EntityAggregateCache<>(
        vertx,
        entityAggregateClass,
        handlerAddress(),
        entityAggregateConfiguration.aggregateCacheTtlInMinutes()
      );
    }
    this.ActorLogic = new ActorLogic<>(
      entityAggregateClass,
      eventBehaviours,
      commandBehaviours,
      entityAggregateConfiguration,
      new Repository<>(EventJournalMapper.INSTANCE, repositoryHandler),
      Boolean.TRUE.equals(entityAggregateConfiguration.snapshots()) ? new Repository<>(AggregateSnapshotMapper.INSTANCE, repositoryHandler) : null,
      new Repository<>(RejectedCommandMapper.INSTANCE, repositoryHandler),
      entityAggregateCache,
      entityAggregateConfiguration.persistenceMode()
    );
    return vertx.eventBus().<JsonObject>consumer(handlerAddress())
      .handler(objectMessage -> {
          final var responseUni = switch (AggregateHandlerAction.valueOf(objectMessage.headers().get(ACTION))) {
            case LOAD -> ActorLogic.load(objectMessage.body().mapTo(EntityAggregateKey.class));
            case COMMAND -> ActorLogic.process(objectMessage.headers().get(CLASS_NAME), objectMessage.body());
            case COMPOSITE_COMMAND ->
              ActorLogic.process(objectMessage.body().mapTo(CompositeCommandWrapper.class));
          };
          responseUni.subscribe()
            .with(
              objectMessage::reply,
              throwable -> {
                if (throwable instanceof VertxServiceException vertxServiceException) {
                  objectMessage.fail(vertxServiceException.error().errorCode(), JsonObject.mapFrom(vertxServiceException.error()).encode());
                } else {
                  LOGGER.error("Unexpected exception raised -> " + objectMessage.body(), throwable);
                  objectMessage.fail(500, JsonObject.mapFrom(new Error(throwable.getMessage(), throwable.getLocalizedMessage(), 500)).encode());
                }
              }
            );
        }
      )
      .exceptionHandler(this::droppedException)
      .completionHandler()
      .onItem().delayIt().by(Duration.of(1, ChronoUnit.SECONDS)) // enough time to
      .invoke(avoid -> Channel.registerHandler());
  }

  private Injector startInjector() {
    this.repositoryHandler = RepositoryHandler.leasePool(config(), vertx);
    moduleBuilder.bind(RepositoryHandler.class).toInstance(repositoryHandler);
    moduleBuilder.bind(Vertx.class).toInstance(vertx);
    moduleBuilder.bind(JsonObject.class).toInstance(config());
    moduleBuilder.bind(EntityAggregateConfiguration.class).toInstance(entityAggregateConfiguration);
    return Injector.of(moduleBuilder.build());
  }

  public static <T extends Entity> List<CommandBehaviourWrapper> loadCommandBehaviours(final Injector injector, Class<T> entityAggregateClass) {
    final var entityAggregateCommandBehaviours = CustomClassLoader.loadFromInjector(injector, Behaviour.class).stream()
      .map(commandBehaviour -> {
        final var genericTypes = parseCommandBehaviourGenericTypes(commandBehaviour.getClass());
        return new CommandBehaviourWrapper(commandBehaviour, genericTypes.getItem1(), genericTypes.getItem2());
      })
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(entityAggregateClass))
      .toList();
    entityAggregateCommandBehaviours.forEach(wrapper -> LOGGER.info(wrapper.delegate().getClass().getSimpleName() + " command behaviour registered for tenant -> " + wrapper.delegate().tenantID()));
    return entityAggregateCommandBehaviours;
  }

  public static<T extends Entity> List<EventBehaviourWrapper> loadEventBehaviours(final Injector injector, Class<T> entityAggregateClass) {
    final var entityAggregateCommandBehaviours = CustomClassLoader.loadFromInjector(injector, Aggregator.class).stream()
      .map(aggregator -> {
          final var genericTypes = parseEventBehaviourTypes(aggregator.getClass());
          return new EventBehaviourWrapper(aggregator, genericTypes.getItem1(), genericTypes.getItem2());
        }
      )
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(entityAggregateClass))
      .toList();
    entityAggregateCommandBehaviours.forEach(eventBehaviour -> LOGGER.info(eventBehaviour.delegate().getClass().getSimpleName() + " event behaviour registered for event " + eventBehaviour.eventClass() + "  tenant -> " + eventBehaviour.delegate().tenantId()));
    return entityAggregateCommandBehaviours;
  }

  public static Tuple2<Class<? extends Entity>, Class<?>> parseEventBehaviourTypes(Class<? extends Aggregator> behaviour) {
    Type[] genericInterfaces = behaviour.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + behaviour.getName());
    } else if (genericInterfaces.length == 0) {
      // should not happen ever.
      throw new IllegalArgumentException("Any event behaviour should implement EventBehaviour interface -> " + behaviour.getName());
    }
    final var genericInterface = genericInterfaces[0];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
      LOGGER.info(behaviour.getName() + " generic types -> " + Arrays.stream(genericTypes).map(Type::getTypeName).toList());
      final Class<? extends Entity> entityClass;
      Class<?> eventClass;
      try {
        entityClass = (Class<? extends Entity>) Class.forName(genericTypes[0].getTypeName());
        eventClass = Class.forName(genericTypes[1].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
      return Tuple2.of(entityClass, eventClass);
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

  public static Tuple2<Class<? extends Entity>, Class<? extends Command>> parseCommandBehaviourGenericTypes(Class<? extends Behaviour> behaviour) {
    Type[] genericInterfaces = behaviour.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + behaviour.getName());
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Behaviours should implement BehaviourCommand interface -> " + behaviour.getName());
    }
    final var genericInterface = genericInterfaces[0];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
      LOGGER.info(behaviour.getName() + " generic types -> " + Arrays.stream(genericTypes).map(Type::getTypeName).toList());
      final Class<? extends Entity> entityClass;
      Class<? extends Command> commandClass;
      try {
        entityClass = (Class<? extends Entity>) Class.forName(genericTypes[0].getTypeName());
        commandClass = (Class<? extends Command>) Class.forName(genericTypes[1].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
      return Tuple2.of(entityClass, commandClass);
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }


  @Override
  public String deploymentID() {
    return verticleUUID;
  }

  private String handlerAddress() {
    return entityAggregateClass.getName() + "." + context.deploymentID();
  }

  private void droppedException(final Throwable throwable) {
    LOGGER.error("[-- AggregateHandlerVerticle " + handlerAddress() + " had to drop the following exception --]", throwable);
  }

  private final String verticleUUID = UUID.randomUUID().toString();

  @Override
  public Uni<Void> asyncStop() {
    LOGGER.info("Stopping " + this.getClass().getSimpleName() + " deploymentID -> " + this.deploymentID());
    Channel.unregisterHandler();
    return repositoryHandler.shutDown();
  }

}
