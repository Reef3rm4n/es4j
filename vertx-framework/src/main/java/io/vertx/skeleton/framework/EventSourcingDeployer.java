package io.vertx.skeleton.framework;

import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.vertx.skeleton.ccp.mappers.MessageQueueSql;
import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.evs.handlers.EventConsumerTask;
import io.vertx.skeleton.evs.handlers.EventSourcingRoute;
import io.vertx.skeleton.orm.LiquibaseHandler;
import io.vertx.skeleton.utils.CustomClassLoader;
import io.vertx.skeleton.evs.*;
import io.vertx.skeleton.evs.handlers.EntityAggregateHandlerVerticle;
import io.vertx.skeleton.orm.RepositoryHandler;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.mutiny.core.Vertx;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import static io.vertx.skeleton.framework.SpineVerticle.LOGGER;
import static io.vertx.skeleton.framework.SpineVerticle.REFLECTIONS;

public class EventSourcingDeployer<T extends EntityAggregate> {
  private final Class<T> entityAggregateClass;

  public EventSourcingDeployer(Class<T> tClass) {
    this.entityAggregateClass = tClass;
  }

  public static Uni<Injector> deploy(Vertx vertx, RepositoryHandler repositoryHandler, final Deque<String> deploymentIds, final Injector injector) {
    final var aggregates = REFLECTIONS.getSubTypesOf(EntityAggregate.class);
    if (!aggregates.isEmpty()) {
      return Multi.createFrom().iterable(aggregates)
        .onItem().transformToUniAndConcatenate(entityAggregateType -> {
            LOGGER.info("Starting eventSourcing for entityAggregate -> " + entityAggregateType.getName());
            final var deployer = new EventSourcingDeployer<>(entityAggregateType)
              .addEventConsumersToMainInjector(injector).addRoutesToModule();
             return deployer.liquibase(repositoryHandler)
              .flatMap(avoid -> deployer.deployHandlers(vertx, repositoryHandler, deploymentIds));
          }
        )
        .collect().asList()
        .replaceWith(injector);
    } else {
      LOGGER.info("Skipping event-sourcing");
      return Uni.createFrom().item(injector);
    }
  }

  private Uni<EventSourcingDeployer<T>> liquibase(final RepositoryHandler repositoryHandler) {
    return LiquibaseHandler.liquibaseString(
      repositoryHandler,
      "event-sourcing.xml",
      Map.of(
        "schema", repositoryHandler.configuration().getString("schema"),
        "entity", entityAggregateClass.getSimpleName().toLowerCase()
      )
    )
      .replaceWith(this);
  }

  public EventSourcingDeployer<T> addEventConsumersToMainInjector(Injector injector) {
    final var consumers = loadConsumers(injector);
    if (!consumers.isEmpty()) {
      consumers.forEach(
        eventConsumer -> injector.putInstance(
          Key.of(EventConsumerTask.class, eventConsumer.getClass().getSimpleName()),
          new EventConsumerTask(entityAggregateClass, eventConsumer, injector.getInstance(RepositoryHandler.class))
        )
      );
    }
    return this;
  }

  public Uni<Void> deployHandlers(Vertx vertx, RepositoryHandler repositoryHandler, final Deque<String> deploymentIds) {
    return vertx.deployVerticle(
        () -> new EntityAggregateHandlerVerticle<>(
          entityAggregateClass,
          SpineVerticle.MODULES
        ),
        new DeploymentOptions().setConfig(repositoryHandler.configuration()).setInstances(CpuCoreSensor.availableProcessors() * 2)
      )
      .map(deploymentIds::add)
      .replaceWithVoid();
  }

  public EventSourcingDeployer<T> addRoutesToModule() {
    final var commandClassMap = getCommandsMap();
    SpineVerticle.MODULES.add(
      new AbstractModule() {
        @Provides
        @Inject
        EventSourcingRoute route(Vertx vertxx) {
          return new EventSourcingRoute(vertxx)
            .setCommandClassMap(commandClassMap)
            .setEntityAggregateClass(entityAggregateClass);
        }
      }
    );
    return this;
  }


  private Map<String, String> getCommandsMap() {
    final var map = new HashMap<String, String>();
    final var commands = getEntityCommands();
    commands.forEach(commandClasses -> {
        map.put(commandClasses.getSimpleName(), commandClasses.getName());
        map.put(commandClasses.getSimpleName().toLowerCase(), commandClasses.getName());
        map.put(commandClasses.getName(), commandClasses.getName());
        map.put(commandClasses.getName().toLowerCase(), commandClasses.getName());
        map.put(MessageQueueSql.camelToSnake(commandClasses.getSimpleName()), commandClasses.getName());
        map.put(MessageQueueSql.camelToSnake(commandClasses.getSimpleName()).toUpperCase(), commandClasses.getName());
      }
    );
    return map;
  }

  private List<EventConsumer> loadConsumers(Injector injector) {
    return CustomClassLoader.loadFromInjector(injector, EventConsumer.class).stream()
      .filter(consumer -> consumer.entity().isAssignableFrom(entityAggregateClass))
      .toList();
  }

  private List<Class<Command>> getEntityCommands() {
    return SpineVerticle.MODULES.stream()
      .map(entry -> entry.getBindings().get().values().stream()
        .map(CustomClassLoader::getFirstGenericType)
        .filter(bindedClass -> CustomClassLoader.getFirstGenericType(bindedClass).isAssignableFrom(entityAggregateClass)
          && CustomClassLoader.getSecondGenericType(bindedClass).isAssignableFrom(Command.class)
        ).toList()
      )
      .flatMap(List::stream)
      .map(EventSourcingDeployer::getSecondGenericType)
      .toList();
  }

  private static Class<Command> getSecondGenericType(Class<?> tClass) {
    Type[] genericInterfaces = tClass.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + tClass.getName());
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Validators should implement CommandValidator interface -> " + tClass.getName());
    }
    final var genericInterface = genericInterfaces[1];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
      try {
        return (Class<Command>) Class.forName(genericTypes[1].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

}
