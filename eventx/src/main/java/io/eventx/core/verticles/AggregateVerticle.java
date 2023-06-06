package io.eventx.core.verticles;


import io.activej.inject.Injector;
import io.activej.inject.module.ModuleBuilder;
import io.eventx.core.objects.*;
import io.eventx.infrastructure.cache.CaffeineAggregateCache;
import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.tuples.Tuple2;
import io.eventx.Event;
import io.eventx.core.CommandHandler;
import io.eventx.core.exceptions.EventxException;
import io.eventx.infrastructure.EventStore;
import io.eventx.infrastructure.Infrastructure;
import io.eventx.infrastructure.OffsetStore;
import io.eventx.infrastructure.bus.AggregateBus;
import io.eventx.infrastructure.misc.CustomClassLoader;
import io.eventx.launcher.EventxMain;
import io.vertx.mutiny.core.Vertx;
import io.eventx.Command;
import io.eventx.CommandBehaviour;
import io.eventx.Aggregate;
import io.eventx.EventBehaviour;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.mutiny.core.eventbus.Message;
import org.crac.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.DeliveryContext;
import org.crac.Resource;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import static io.eventx.core.CommandHandler.camelToKebab;

public class AggregateVerticle<T extends Aggregate> extends AbstractVerticle implements Resource {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateVerticle.class);

  public static final String ACTION = "action";
  private final ModuleBuilder moduleBuilder;
  private final Class<T> aggregateClass;
  private final String deploymentID;
  private AggregateConfiguration aggregateConfiguration;
  private CommandHandler<T> logic;
  private List<BehaviourWrapper> behaviourWrappers;
  private List<AggregatorWrapper> aggregatorWrappers;
  private Infrastructure infrastructure;

  public AggregateVerticle(
    final Class<T> aggregateClass,
    final ModuleBuilder moduleBuilder,
    String deploymentID
  ) {
    this.aggregateClass = aggregateClass;
    this.moduleBuilder = moduleBuilder;
    this.deploymentID = deploymentID;
  }

  @Override
  public Uni<Void> asyncStart() {
    final var injector = startInjector();
    this.aggregateConfiguration = config().getJsonObject(aggregateClass.getSimpleName(), new JsonObject()).mapTo(AggregateConfiguration.class);
    LOGGER.info("Event.x starting {}::{}", aggregateClass.getSimpleName(), this.deploymentID);
    this.aggregatorWrappers = loadAggregators(injector, aggregateClass);
    this.behaviourWrappers = loadBehaviours(injector, aggregateClass);
    this.infrastructure = new Infrastructure(
      new CaffeineAggregateCache(),
      injector.getInstance(EventStore.class),
      injector.getInstance(OffsetStore.class)
    );
    vertx.eventBus().addInboundInterceptor(this::addContextualData);
    this.logic = new CommandHandler<>(
      vertx,
      aggregateClass,
      aggregatorWrappers,
      behaviourWrappers,
      infrastructure,
      aggregateConfiguration
    );
    return AggregateBus.registerCommandConsumer(
        vertx,
        aggregateClass,
        deploymentID,
        this::processCommand
      )
      .flatMap(avoid -> AggregateBus.waitForRegistration(deploymentID, aggregateClass));
  }

  private void processCommand(Message<JsonObject> jsonMessage) {
    LOGGER.debug("Incoming command {}", jsonMessage.body().encodePrettily());
    logic.process(jsonMessage.body().getString("commandClass"), jsonMessage.body().getJsonObject("command"))
      .subscribe()
      .with(
        jsonMessage::reply,
        throwable -> {
          if (throwable instanceof EventxException vertxServiceException) {
            jsonMessage.fail(vertxServiceException.error().externalErrorCode(), JsonObject.mapFrom(vertxServiceException.error()).encodePrettily());
          } else {
            LOGGER.error("Unexpected exception raised", throwable);
            jsonMessage.fail(500, JsonObject.mapFrom(new EventxError(throwable.getMessage(), throwable.getLocalizedMessage(), 500)).encode());
          }
        }
      );
  }

  private void addContextualData(DeliveryContext<Object> event) {
    ContextualData.put("AGGREGATE", aggregateClass.getSimpleName());
    event.next();
  }

  private Injector startInjector() {
    moduleBuilder.bind(Vertx.class).toInstance(vertx);
    final var config = config().put("schema", camelToKebab(aggregateClass.getSimpleName()));
    moduleBuilder.bind(JsonObject.class).toInstance(config);
    moduleBuilder.bind(AggregateConfiguration.class).toInstance(aggregateConfiguration);
    return Injector.of(moduleBuilder.build());
  }

  public static <T extends Aggregate> List<BehaviourWrapper> loadBehaviours(final Injector injector, Class<T> entityAggregateClass) {
    final var behaviours = CustomClassLoader.loadFromInjector(injector, CommandBehaviour.class).stream()
      .filter(behaviour ->
        parseCommandBehaviourGenericTypes(behaviour.getClass()).getItem1().isAssignableFrom(entityAggregateClass))
      .map(commandBehaviour -> {
          final var genericTypes = parseCommandBehaviourGenericTypes(commandBehaviour.getClass());
          return new BehaviourWrapper(commandBehaviour, entityAggregateClass, genericTypes.getItem2());
        }
      )
      .toList();
    if (behaviours.isEmpty()) {
      throw new IllegalStateException("Behaviours not found for " + entityAggregateClass.getSimpleName());
    }
    if (!EventxMain.AGGREGATE_COMMANDS.containsKey(entityAggregateClass)) {
      final var behavioursClass = new ArrayList<Class<? extends Command>>(behaviours.stream()
        .map(wrapper -> (Class<? extends Command>) wrapper.commandClass())
        .toList());
      behavioursClass.add(LoadAggregate.class);
      EventxMain.AGGREGATE_COMMANDS.put(
        entityAggregateClass,
        behavioursClass
      );
    }
    return behaviours;
  }

  public static <T extends Aggregate> List<AggregatorWrapper> loadAggregators(final Injector injector, Class<T> entityAggregateClass) {
    final var aggregators = CustomClassLoader.loadFromInjector(injector, EventBehaviour.class).stream()
      .map(aggregator -> {
          final var genericTypes = parseAggregatorClass(aggregator.getClass());
          return new AggregatorWrapper(aggregator, genericTypes.getItem1(), genericTypes.getItem2());
        }
      )
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(entityAggregateClass))
      .toList();
    if (aggregators.isEmpty()) {
      throw new IllegalStateException("Aggregators not found for " + entityAggregateClass.getSimpleName());
    }
    EventxMain.AGGREGATE_EVENTS.putIfAbsent(
      entityAggregateClass, aggregators.stream()
        .map(wrapper -> (Class<Event>) wrapper.eventClass())
        .toList()
    );
    return aggregators;
  }

  public static Tuple2<Class<? extends Aggregate>, Class<?>> parseAggregatorClass(Class<? extends EventBehaviour> behaviour) {
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
      final Class<? extends Aggregate> entityClass;
      Class<?> eventClass;
      try {
        entityClass = (Class<? extends Aggregate>) Class.forName(genericTypes[0].getTypeName());
        eventClass = Class.forName(genericTypes[1].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
      return Tuple2.of(entityClass, eventClass);
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

  public static Tuple2<Class<? extends Aggregate>, Class<? extends Command>> parseCommandBehaviourGenericTypes(Class<? extends CommandBehaviour> behaviour) {
    Type[] genericInterfaces = behaviour.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + behaviour.getName());
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Behaviours should implement BehaviourCommand interface -> " + behaviour.getName());
    }
    final var genericInterface = genericInterfaces[0];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
      final Class<? extends Aggregate> entityClass;
      Class<? extends Command> commandClass;
      try {
        entityClass = (Class<? extends Aggregate>) Class.forName(genericTypes[0].getTypeName());
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
  public Uni<Void> asyncStop() {
    LOGGER.info("Stopping {} {}", aggregateClass.getSimpleName(), deploymentID);
    AggregateBus.stop(vertx, aggregateClass, deploymentID);
    return infrastructure.stop();
  }


  @Override
  public void beforeCheckpoint(Context<? extends Resource> context) throws Exception {

  }

  @Override
  public void afterRestore(Context<? extends Resource> context) throws Exception {

  }

}
