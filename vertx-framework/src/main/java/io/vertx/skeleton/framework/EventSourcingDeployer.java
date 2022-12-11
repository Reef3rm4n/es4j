package io.vertx.skeleton.framework;

import io.vertx.skeleton.utils.CustomClassLoader;
import io.vertx.skeleton.evs.*;
import io.vertx.skeleton.evs.cache.EntityAggregateSynchronizer;
import io.vertx.skeleton.evs.handlers.EntityAggregateConfiguration;
import io.vertx.skeleton.evs.handlers.EntityAggregateHandlerVerticle;
import io.vertx.skeleton.evs.objects.*;
import io.vertx.skeleton.orm.RepositoryHandler;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;

import static io.vertx.skeleton.framework.SpineVerticle.LOGGER;
import static io.vertx.skeleton.framework.SpineVerticle.REFLECTIONS;

public class EventSourcingDeployer<T extends EntityAggregate> {
  private final Class<T> entityAggregateClass;

  public EventSourcingDeployer(Class<T> tClass) {
    this.entityAggregateClass = tClass;
  }

  public static Uni<Injector> deploy(Vertx vertx, RepositoryHandler repositoryHandler, final Deque<String> deploymentIds, final Injector injector) {
    final var aggregates = REFLECTIONS.getSubTypesOf(EntityAggregate.class);
    if (! aggregates.isEmpty()) {
      return Multi.createFrom().iterable(aggregates)
        .onItem().transformToUniAndConcatenate(entityAggregateType -> {
            LOGGER.info("Starting eventSourcing for entityAggregate -> " + entityAggregateType.getName());
            return new EventSourcingDeployer<>(entityAggregateType)
              .performEntityAggregateLoading(vertx, repositoryHandler, deploymentIds, injector);
          }
        )
        .collect().asList()
        .replaceWith(injector);
    } else {
      return Uni.createFrom().item(injector);
    }
  }

  public Uni<Void> performEntityAggregateLoading(Vertx vertx, RepositoryHandler repositoryHandler, final Deque<String> deploymentIds, final Injector injector) {
    final var commandBehaviours = loadCommandBehaviours(injector);
    final var eventBehaviours = loadEventBehaviours(injector);
    final var commandValidators = loadCommandValidators(injector);
    final var queryBehaviours = loadQueryBehaviours(injector);
    final var projections = loadProjections(injector);
//    final var eventConsumers = loadEventConsumers(injector);
    final var routes = loadRoutes(injector);
    final var aggregateConfiguration = repositoryHandler.configuration().getJsonObject(entityAggregateClass.getSimpleName());
    injector.putInstance(Key.of(JsonObject.class,entityAggregateClass.getSimpleName()),aggregateConfiguration);
    if (aggregateConfiguration == null) {
      throw new IllegalStateException("Aggregate configuration not found -> " + entityAggregateClass.getSimpleName());
    }
    Supplier<Verticle> verticleSupplier = () -> new EntityAggregateHandlerVerticle<>(
      entityAggregateClass,
      commandValidators,
      commandBehaviours,
      eventBehaviours,
      queryBehaviours,
      projections,
      aggregateConfiguration.mapTo(EntityAggregateConfiguration.class)
    );
    return vertx.deployVerticle(
        verticleSupplier,
        new DeploymentOptions().setConfig(repositoryHandler.configuration()).setInstances(CpuCoreSensor.availableProcessors() * 2)
      )
      .map(deploymentIds::add)
      .flatMap(avoid -> vertx.deployVerticle(
          new EntityAggregateSynchronizer<>(entityAggregateClass),
          new DeploymentOptions()
            .setConfig(repositoryHandler.configuration())
            .setInstances(1)
        )
      )
      .map(deploymentIds::add)
      .flatMap(avoid -> {
          if (! routes.isEmpty()) {
            return vertx.deployVerticle(new EventSourcingHttpRouter(routes, entityAggregateClass), new DeploymentOptions().setConfig(repositoryHandler.configuration()).setInstances(CpuCoreSensor.availableProcessors() * 2))
              .map(deploymentIds::add);
          }
          return Uni.createFrom().voidItem();
        }
      )
      .replaceWithVoid();
  }

  private List<? extends EventSourcingHttpRoute> loadRoutes(final Injector injector) {
    return CustomClassLoader.loadFromInjector(injector, EventSourcingHttpRoute.class).stream()
      .map(httpRoute -> new VertxEventSourcingRouteWrapper<>(httpRoute, parseRouteGenericType(httpRoute.getClass())))
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(entityAggregateClass))
      .map(VertxEventSourcingRouteWrapper::route)
      .toList();
  }


  private List<CommandValidatorWrapper> loadCommandValidators(Injector injector) {
    final var entityAggregateCommandBehaviours = CustomClassLoader.loadFromInjector(injector, CommandValidator.class).stream()
      .map(commandValidator ->  {
        final var genericTypes = parseCommandValidatorGenericTypes(commandValidator.getClass());
        return new CommandValidatorWrapper(commandValidator, genericTypes.getItem1(), genericTypes.getItem2());
      }
      )
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(entityAggregateClass))
      .toList();
    entityAggregateCommandBehaviours.forEach(projection -> LOGGER.info(projection.getClass().getSimpleName() + " command validator registered for tenant -> " + projection.delegate().tenant()));
    return entityAggregateCommandBehaviours;
  }

  private List<QueryBehaviourWrapper> loadQueryBehaviours(Injector injector) {
    final var entityAggregateCommandBehaviours = CustomClassLoader.loadFromInjector(injector, QueryBehaviour.class).stream()
      .map(queryBehaviour ->  {
        final var genericTypes = parseQueryBehaviourGenericTypes(queryBehaviour.getClass());
        return new QueryBehaviourWrapper(queryBehaviour, genericTypes.getItem1(), genericTypes.getItem2());
      })
      .filter(behaviour -> behaviour.delegate().getClass().getName().equals(entityAggregateClass.getName()))
      .toList();
    entityAggregateCommandBehaviours.forEach(projection -> LOGGER.info(projection.getClass().getSimpleName() + " query behaviour registered for tenant -> " + projection.delegate().tenant()));
    return entityAggregateCommandBehaviours;
  }

  private List<CommandBehaviourWrapper> loadCommandBehaviours(final Injector injector) {
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

  private List<EventBehaviourWrapper> loadEventBehaviours(final Injector injector) {
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


  private List<ProjectionWrapper> loadProjections(Injector injector) {
    final var entityAggregateProjections = CustomClassLoader.loadFromInjector(injector, Projection.class).stream()
      .map(projection -> {
        final var genericType = parseProjectionGenericType(projection.getClass());
        return new ProjectionWrapper(projection, genericType);
      })
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(entityAggregateClass))
      .toList();
    entityAggregateProjections.forEach(projection -> LOGGER.info(projection.getClass().getSimpleName() + " projection registered for tenant -> " + projection.delegate().tenant()));
    return entityAggregateProjections;
  }

  public Tuple2<Class<? extends EntityAggregate>, Class<? extends EntityAggregateQuery>> parseQueryBehaviourGenericTypes(Class<? extends QueryBehaviour> behaviour) {
    Type[] genericInterfaces = behaviour.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + behaviour.getName());
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Validators should implement CommandValidator interface -> " + behaviour.getName());
    }
    final var genericInterface = genericInterfaces[0];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
//      LOGGER.info(behaviour.getName() + " generic types -> " + Arrays.stream(genericTypes).map(Type::getTypeName).toList());
      final Class<? extends EntityAggregate> entityClass;
      Class<? extends EntityAggregateQuery> queryClass;
      try {
        entityClass = (Class<? extends EntityAggregate>) Class.forName(genericTypes[0].getTypeName());
        queryClass = (Class<? extends EntityAggregateQuery>) Class.forName(genericTypes[1].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
      return Tuple2.of(entityClass, queryClass);
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }


  private ProjectionWrapper instantiateProjection(Class<?> tClass) {
    try {
      final var tProjection = (Projection<T>) tClass.getDeclaredConstructor().newInstance();
      final var genericType = parseProjectionGenericType(tProjection.getClass());
      return new ProjectionWrapper(tProjection, genericType);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public Class<? extends EntityAggregate> parseProjectionGenericType(Class<? extends Projection> projection) {
    Type[] genericInterfaces = projection.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + projection.getName());
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Validators should implement CommandValidator interface -> " + projection.getName());
    }
    final var genericInterface = genericInterfaces[0];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
//      LOGGER.info(projection.getName() + " generic types -> " + Arrays.stream(genericTypes).map(Type::getTypeName).toList());
      final Class<? extends EntityAggregate> entityClass;
      try {
        entityClass = (Class<? extends EntityAggregate>) Class.forName(genericTypes[0].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
      return entityClass;
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

  public Tuple2<Class<? extends EntityAggregate>, Class<? extends EntityAggregateCommand>> parseCommandValidatorGenericTypes(Class<? extends CommandValidator> behaviour) {
    Type[] genericInterfaces = behaviour.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + behaviour.getName());
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Validators should implement CommandValidator interface -> " + behaviour.getName());
    }
    final var genericInterface = genericInterfaces[0];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
//      LOGGER.info(behaviour.getName() + " generic types -> " + Arrays.stream(genericTypes).map(Type::getTypeName).toList());
      final Class<? extends EntityAggregate> entityClass;
      Class<? extends EntityAggregateCommand> commandClass;
      try {
        entityClass = (Class<? extends EntityAggregate>) Class.forName(genericTypes[0].getTypeName());
        commandClass = (Class<? extends EntityAggregateCommand>) Class.forName(genericTypes[1].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
      return Tuple2.of(entityClass, commandClass);
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

  public Tuple2<Class<? extends EntityAggregate>, Class<?>> parseEventBehaviourTypes(Class<? extends EventBehaviour> behaviour) {
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

  public Tuple2<Class<? extends EntityAggregate>, Class<? extends EntityAggregateCommand>> parseCommandBehaviourGenericTypes(Class<? extends CommandBehaviour> behaviour) {
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
      Class<? extends EntityAggregateCommand> commandClass;
      try {
        entityClass = (Class<? extends EntityAggregate>) Class.forName(genericTypes[0].getTypeName());
        commandClass = (Class<? extends EntityAggregateCommand>) Class.forName(genericTypes[1].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
      return Tuple2.of(entityClass, commandClass);
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

  private Class<? extends EntityAggregate> parseRouteGenericType(Class<? extends EventSourcingHttpRoute> behaviour) {
    Type[] genericInterfaces = behaviour.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + behaviour.getName());
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Validators should implement CommandValidator interface -> " + behaviour.getName());
    }
    final var genericInterface = genericInterfaces[0];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
      LOGGER.info(behaviour.getName() + " generic types -> " + Arrays.stream(genericTypes).map(Type::getTypeName).toList());
      try {
        return (Class<? extends EntityAggregate>) Class.forName(genericTypes[0].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

}
