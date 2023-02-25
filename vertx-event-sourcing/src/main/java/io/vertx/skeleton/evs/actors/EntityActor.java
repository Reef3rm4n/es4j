package io.vertx.skeleton.evs.actors;

import io.activej.inject.Injector;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.core.Vertx;
import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.evs.Behaviour;
import io.vertx.skeleton.evs.Entity;
import io.vertx.skeleton.evs.Aggregator;
import io.vertx.skeleton.evs.cache.EntityCache;
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

import static io.vertx.skeleton.evs.actors.Channel.registerActor;

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
  private final String deploymentID;
  private List<BehaviourWrapper> behaviourWrappers;
  private List<AggregatorWrapper> aggregatorWrappers;
  private final ModuleBuilder moduleBuilder;
  private EntityCache<T, EntityState<T>> entityCache = null;
  private final Class<T> entityClass;
  private EntityConfiguration entityConfiguration;
  private RepositoryHandler repositoryHandler;
  public ActorLogic<T> logic;

  public EntityActor(
    final String deploymentID,
    final Class<T> entityClass,
    final ModuleBuilder moduleBuilder
  ) {
    this.deploymentID = deploymentID;
    this.entityClass = entityClass;
    this.moduleBuilder = moduleBuilder;
  }

  @Override
  public Uni<Void> asyncStart() {
    this.entityConfiguration = config().getJsonObject(entityClass.getSimpleName(), new JsonObject()).mapTo(EntityConfiguration.class);
    LOGGER.info("Starting " + entityClass.getSimpleName() + " [address:" + Channel.actorAddress(entityClass, deploymentID) + "]");
    final var injector = startInjector();
    this.aggregatorWrappers = loadAggregators(injector, entityClass);
    this.behaviourWrappers = loadBehaviours(injector, entityClass);
    if (Boolean.TRUE.equals(entityConfiguration.useCache())) {
      this.entityCache = new EntityCache<>(
        vertx,
        entityClass,
        entityConfiguration.aggregateCacheTtlInMinutes()
      );
    }
    this.logic = new ActorLogic<>(
      entityClass,
      aggregatorWrappers,
      behaviourWrappers,
      entityConfiguration,
      new Repository<>(EventJournalMapper.INSTANCE, repositoryHandler),
      Boolean.TRUE.equals(entityConfiguration.snapshots()) ? new Repository<>(AggregateSnapshotMapper.INSTANCE, repositoryHandler) : null,
      new Repository<>(RejectedCommandMapper.INSTANCE, repositoryHandler),
      entityCache
    );
    return vertx.eventBus().<JsonObject>consumer(Channel.actorAddress(entityClass, deploymentID))
      .handler(jsonMessage -> {
          final var responseUni = switch (ActorCommand.valueOf(jsonMessage.headers().get(ACTION))) {
            case LOAD -> logic.load(jsonMessage.body().mapTo(EntityKey.class));
            case COMMAND -> logic.process(jsonMessage.headers().get(CLASS_NAME), jsonMessage.body());
            case COMPOSITE_COMMAND -> logic.process(jsonMessage.body().mapTo(CompositeCommandWrapper.class));
          };
          responseUni.subscribe()
            .with(
              jsonMessage::reply,
              throwable -> {
                if (throwable instanceof VertxServiceException vertxServiceException) {
                  jsonMessage.fail(vertxServiceException.error().errorCode(), JsonObject.mapFrom(vertxServiceException.error()).encode());
                } else {
                  LOGGER.error("Unexpected exception raised -> " + jsonMessage.body(), throwable);
                  jsonMessage.fail(500, JsonObject.mapFrom(new Error(throwable.getMessage(), throwable.getLocalizedMessage(), 500)).encode());
                }
              }
            );
        }
      )
      .exceptionHandler(this::droppedException)
      .completionHandler()
      .onItem().delayIt().by(Duration.of(1, ChronoUnit.SECONDS));
  }

  private Injector startInjector() {
    this.repositoryHandler = RepositoryHandler.leasePool(config(), vertx);
    moduleBuilder.bind(RepositoryHandler.class).toInstance(repositoryHandler);
    moduleBuilder.bind(Vertx.class).toInstance(vertx);
    moduleBuilder.bind(JsonObject.class).toInstance(config());
    moduleBuilder.bind(EntityConfiguration.class).toInstance(entityConfiguration);
    return Injector.of(moduleBuilder.build());
  }

  public static <T extends Entity> List<BehaviourWrapper> loadBehaviours(final Injector injector, Class<T> entityAggregateClass) {
    final var entityAggregateCommandBehaviours = CustomClassLoader.loadFromInjector(injector, Behaviour.class).stream()
      .map(commandBehaviour -> {
        final var genericTypes = parseCommandBehaviourGenericTypes(commandBehaviour.getClass());
        return new BehaviourWrapper(commandBehaviour, genericTypes.getItem1(), genericTypes.getItem2());
      })
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(entityAggregateClass))
      .toList();
    entityAggregateCommandBehaviours.forEach(wrapper -> LOGGER.info(wrapper.delegate().getClass().getSimpleName() + " command behaviour registered for tenant -> " + wrapper.delegate().tenantID()));
    return entityAggregateCommandBehaviours;
  }

  public static <T extends Entity> List<AggregatorWrapper> loadAggregators(final Injector injector, Class<T> entityAggregateClass) {
    final var entityAggregateCommandBehaviours = CustomClassLoader.loadFromInjector(injector, Aggregator.class).stream()
      .map(aggregator -> {
          final var genericTypes = parseAggregatorClass(aggregator.getClass());
          return new AggregatorWrapper(aggregator, genericTypes.getItem1(), genericTypes.getItem2());
        }
      )
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(entityAggregateClass))
      .toList();
    entityAggregateCommandBehaviours.forEach(eventBehaviour -> LOGGER.info(eventBehaviour.delegate().getClass().getSimpleName() + " event behaviour registered for event " + eventBehaviour.eventClass() + "  tenant -> " + eventBehaviour.delegate().tenantId()));
    return entityAggregateCommandBehaviours;
  }

  public static Tuple2<Class<? extends Entity>, Class<?>> parseAggregatorClass(Class<? extends Aggregator> behaviour) {
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

  private void droppedException(final Throwable throwable) {
    LOGGER.error("[-- " + entityClass.getSimpleName() + " had to drop the following exception --]", throwable);
  }

  @Override
  public Uni<Void> asyncStop() {
    LOGGER.info("Stopping " + entityClass.getSimpleName());
    Channel.killActor(vertx, entityClass, deploymentID);
    return repositoryHandler.shutDown();
  }

}
