package io.vertx.eventx.handlers;

import io.activej.inject.Injector;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.eventx.infrastructure.Infra;
import io.vertx.eventx.infrastructure.PgInfrastructure;
import io.vertx.eventx.infrastructure.bus.AggregateBus;
import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventxException;
import io.vertx.eventx.objects.*;
import io.vertx.eventx.infrastructure.pg.models.AggregateRecordKey;
import io.vertx.mutiny.core.Vertx;
import io.vertx.eventx.Command;
import io.vertx.eventx.Behaviour;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Aggregator;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.common.CustomClassLoader;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
public class AggregateVerticle<T extends Aggregate> extends AbstractVerticle {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateVerticle.class);

  public static final String ACTION = "action";
  public static final String CLASS_NAME = "className";
  private final ModuleBuilder moduleBuilder;
  private final Class<T> aggregateClass;
  private AggregateConfiguration aggregateConfiguration;
  private AggregateLogic<T> logic;
  private List<BehaviourWrapper> behaviourWrappers;
  private List<AggregatorWrapper> aggregatorWrappers;
  private Infra<T> pgInfrastructure;

  public AggregateVerticle(
    final Class<T> aggregateClass,
    final ModuleBuilder moduleBuilder
  ) {
    this.aggregateClass = aggregateClass;
    this.moduleBuilder = moduleBuilder;
  }

  @Override
  public Uni<Void> asyncStart() {
    this.aggregateConfiguration = config().getJsonObject(aggregateClass.getSimpleName(), new JsonObject()).mapTo(AggregateConfiguration.class);
    LOGGER.info("Starting Entity Actor " + aggregateClass.getSimpleName());
    final var injector = startInjector();
    this.aggregatorWrappers = loadAggregators(injector, aggregateClass);
    this.behaviourWrappers = loadBehaviours(injector, aggregateClass);
    this.pgInfrastructure = injector.getInstance(Infra.class);
    this.logic = new AggregateLogic<>(
      aggregateClass,
      aggregatorWrappers,
      behaviourWrappers,
      aggregateConfiguration,
      pgInfrastructure
    );
    return AggregateBus.registerCommandConsumer(
      vertx,
      aggregateClass,
      this.deploymentID(),
      jsonMessage -> {
        LOGGER.info("Incoming command " + jsonMessage.body().encodePrettily());
        final var responseUni = switch (Action.valueOf(jsonMessage.headers().get(ACTION))) {
          case LOAD -> logic.fetch(jsonMessage.body().mapTo(AggregateRecordKey.class));
          case COMMAND -> logic.process(jsonMessage.headers().get(CLASS_NAME), jsonMessage.body());
        };
        responseUni.subscribe()
          .with(
            jsonMessage::reply,
            throwable -> {
              if (throwable instanceof EventxException vertxServiceException) {
                jsonMessage.fail(vertxServiceException.error().internalErrorCode(), JsonObject.mapFrom(vertxServiceException.error()).encodePrettily());
              } else {
                LOGGER.error("Unexpected exception raised -> " + jsonMessage.body(), throwable);
                jsonMessage.fail(500, JsonObject.mapFrom(new EventXError(throwable.getMessage(), throwable.getLocalizedMessage(), 500)).encode());
              }
            }
          );
      }
    );
  }

  private Injector startInjector() {
    moduleBuilder.bind(Vertx.class).toInstance(vertx);
    moduleBuilder.bind(JsonObject.class).toInstance(config());
    moduleBuilder.bind(AggregateConfiguration.class).toInstance(aggregateConfiguration);
    return Injector.of(moduleBuilder.build());
  }

  public static <T extends Aggregate> List<BehaviourWrapper> loadBehaviours(final Injector injector, Class<T> entityAggregateClass) {
    final var behaviours = CustomClassLoader.loadFromInjector(injector, Behaviour.class).stream()
      .map(commandBehaviour -> {
        final var genericTypes = parseCommandBehaviourGenericTypes(commandBehaviour.getClass());
        return new BehaviourWrapper(commandBehaviour, genericTypes.getItem1(), genericTypes.getItem2());
      })
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(entityAggregateClass))
      .toList();
    behaviours.forEach(behaviour -> LOGGER.info(
      new JsonObject()
        .put("aggregator", behaviour.delegate().getClass().getName())
        .put("event", behaviour.commandClass().getName())
        .put("tenantId", behaviour.delegate().tenantID())
        .encodePrettily()
    ));
    return behaviours;
  }

  public static <T extends Aggregate> List<AggregatorWrapper> loadAggregators(final Injector injector, Class<T> entityAggregateClass) {
    final var aggregators = CustomClassLoader.loadFromInjector(injector, Aggregator.class).stream()
      .map(aggregator -> {
          final var genericTypes = parseAggregatorClass(aggregator.getClass());
          return new AggregatorWrapper(aggregator, genericTypes.getItem1(), genericTypes.getItem2());
        }
      )
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(entityAggregateClass))
      .toList();
    aggregators.forEach(eventBehaviour -> LOGGER.info(
      new JsonObject()
        .put("aggregator", eventBehaviour.delegate().getClass().getName())
        .put("event", eventBehaviour.eventClass().getName())
        .put("tenantId", eventBehaviour.delegate().tenantId())
        .encodePrettily()
    ));
    return aggregators;
  }

  public static Tuple2<Class<? extends Aggregate>, Class<?>> parseAggregatorClass(Class<? extends Aggregator> behaviour) {
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

  public static Tuple2<Class<? extends Aggregate>, Class<? extends Command>> parseCommandBehaviourGenericTypes(Class<? extends Behaviour> behaviour) {
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
    LOGGER.info("Stopping " + aggregateClass.getSimpleName());
    AggregateBus.killActor(vertx, aggregateClass, this.deploymentID());
    LOGGER.info("[deploymentIDs:" + vertx.deploymentIDs() + "]");
    LOGGER.info("[contextID:" + context.deploymentID() + "]");
    return pgInfrastructure.stop();
  }


}
