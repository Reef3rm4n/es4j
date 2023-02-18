package io.vertx.skeleton.evs.handlers;

import io.activej.inject.Injector;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.core.Vertx;
import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.evs.CommandBehaviour;
import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.EventBehaviour;
import io.vertx.skeleton.evs.cache.EntityAggregateCache;
import io.vertx.skeleton.evs.objects.*;
import io.vertx.skeleton.models.exceptions.VertxServiceException;
import io.vertx.skeleton.orm.Repository;
import io.vertx.skeleton.orm.RepositoryHandler;
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

public class EntityAggregateHandlerVerticle<T extends EntityAggregate> extends AbstractVerticle {
  protected static final Logger LOGGER = LoggerFactory.getLogger(EntityAggregateHandlerVerticle.class);
  public static final String ACTION = "action";
  public static final String CLASS_NAME = "className";
  private List<CommandBehaviourWrapper> commandBehaviours;
  private List<EventBehaviourWrapper> eventBehaviours;
  private final Collection<Module> modules;
  private EntityAggregateCache<T, EntityAggregateState<T>> entityAggregateCache = null;
  private AggregateHandlerHeartBeat<T> aggregateHandlerHeartBeat;
  private final Class<T> entityAggregateClass;
  private EntityAggregateConfiguration entityAggregateConfiguration;
  private RepositoryHandler repositoryHandler;
  public EntityAggregateHandler<T> entityAggregateHandler;

  public EntityAggregateHandlerVerticle(
    final Class<T> entityAggregateClass,
    final Collection<Module> modules
  ) {
    this.entityAggregateClass = entityAggregateClass;
    this.modules = modules;
  }

  @Override
  public Uni<Void> asyncStart() {
    LOGGER.info("Starting " + this.getClass().getSimpleName() + " " + context.deploymentID() + " configuration -> " + JsonObject.mapFrom(entityAggregateConfiguration).encodePrettily());
    final var injector = getInjector(modules);
    this.eventBehaviours = loadEventBehaviours(injector, entityAggregateClass);
    this.commandBehaviours = loadCommandBehaviours(injector, entityAggregateClass);
    this.aggregateHandlerHeartBeat = new AggregateHandlerHeartBeat<>(vertx, entityAggregateConfiguration.handlerHeartBeatInterval(), entityAggregateClass, handlerAddress());
    if (Boolean.TRUE.equals(entityAggregateConfiguration.useCache())) {
      this.entityAggregateCache = new EntityAggregateCache<>(
        vertx,
        entityAggregateClass,
        handlerAddress(),
        entityAggregateConfiguration.aggregateCacheTtlInMinutes()
      );
    }
    this.entityAggregateHandler = new EntityAggregateHandler<>(
      entityAggregateClass,
      eventBehaviours,
      commandBehaviours,
      entityAggregateConfiguration,
      new Repository<>(new EventJournalMapper(entityAggregateClass), repositoryHandler),
      Boolean.TRUE.equals(entityAggregateConfiguration.snapshots()) ? new Repository<>(new AggregateSnapshotMapper(entityAggregateClass), repositoryHandler) : null,
      new Repository<>(new RejectedCommandMapper(entityAggregateClass), repositoryHandler),
      entityAggregateCache,
      entityAggregateConfiguration.persistenceMode()
    );
    return vertx.eventBus().<JsonObject>consumer(handlerAddress())
      .handler(objectMessage -> {
          final var responseUni = switch (AggregateHandlerAction.valueOf(objectMessage.headers().get(ACTION))) {
            case LOAD -> entityAggregateHandler.load(objectMessage.body().mapTo(EntityAggregateKey.class));
            case COMMAND -> entityAggregateHandler.process(objectMessage.headers().get(CLASS_NAME), objectMessage.body());
            case COMPOSITE_COMMAND ->
              entityAggregateHandler.process(objectMessage.body().mapTo(CompositeCommandWrapper.class));
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
      .invoke(avoid -> aggregateHandlerHeartBeat.registerHandler());
  }

  private Injector getInjector(Collection<Module> modules) {
    final var moduleBuilder = ModuleBuilder.create().install(modules);
    this.repositoryHandler = RepositoryHandler.leasePool(config(), vertx);
    this.entityAggregateConfiguration = repositoryHandler.configuration().getJsonObject(entityAggregateClass.getSimpleName()).mapTo(EntityAggregateConfiguration.class);
    moduleBuilder.bind(RepositoryHandler.class).toInstance(repositoryHandler);
    moduleBuilder.bind(Vertx.class).toInstance(vertx);
    moduleBuilder.bind(JsonObject.class).toInstance(config());
    moduleBuilder.bind(EntityAggregateConfiguration.class).toInstance(entityAggregateConfiguration);
    return Injector.of(moduleBuilder.build());
  }

  public static <T extends EntityAggregate> List<CommandBehaviourWrapper> loadCommandBehaviours(final Injector injector, Class<T> entityAggregateClass) {
    final var entityAggregateCommandBehaviours = CustomClassLoader.loadFromInjector(injector, CommandBehaviour.class).stream()
      .map(commandBehaviour -> {
        final var genericTypes = parseCommandBehaviourGenericTypes(commandBehaviour.getClass());
        return new CommandBehaviourWrapper(commandBehaviour, genericTypes.getItem1(), genericTypes.getItem2());
      })
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(entityAggregateClass))
      .toList();
    entityAggregateCommandBehaviours.forEach(wrapper -> LOGGER.info(wrapper.delegate().getClass().getSimpleName() + " command behaviour registered for tenant -> " + wrapper.delegate().tenant()));
    return entityAggregateCommandBehaviours;
  }

  public static<T extends EntityAggregate> List<EventBehaviourWrapper> loadEventBehaviours(final Injector injector,Class<T> entityAggregateClass) {
    final var entityAggregateCommandBehaviours = CustomClassLoader.loadFromInjector(injector, EventBehaviour.class).stream()
      .map(eventBehaviour -> {
          final var genericTypes = parseEventBehaviourTypes(eventBehaviour.getClass());
          return new EventBehaviourWrapper(eventBehaviour, genericTypes.getItem1(), genericTypes.getItem2());
        }
      )
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(entityAggregateClass))
      .toList();
    entityAggregateCommandBehaviours.forEach(eventBehaviour -> LOGGER.info(eventBehaviour.delegate().getClass().getSimpleName() + " event behaviour registered for event " + eventBehaviour.eventClass() + "  tenant -> " + eventBehaviour.delegate().tenant()));
    return entityAggregateCommandBehaviours;
  }

  public static Tuple2<Class<? extends EntityAggregate>, Class<?>> parseEventBehaviourTypes(Class<? extends EventBehaviour> behaviour) {
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
      final Class<? extends EntityAggregate> entityClass;
      Class<?> eventClass;
      try {
        entityClass = (Class<? extends EntityAggregate>) Class.forName(genericTypes[0].getTypeName());
        eventClass = Class.forName(genericTypes[1].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
      return Tuple2.of(entityClass, eventClass);
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

  public static Tuple2<Class<? extends EntityAggregate>, Class<? extends io.vertx.skeleton.evs.Command>> parseCommandBehaviourGenericTypes(Class<? extends CommandBehaviour> behaviour) {
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
      final Class<? extends EntityAggregate> entityClass;
      Class<? extends io.vertx.skeleton.evs.Command> commandClass;
      try {
        entityClass = (Class<? extends EntityAggregate>) Class.forName(genericTypes[0].getTypeName());
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
    aggregateHandlerHeartBeat.unregisterHandler();
    return repositoryHandler.shutDown();
  }

}
