package io.vertx.eventx.handlers;

import io.activej.inject.Injector;
import io.activej.inject.module.ModuleBuilder;
import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.eventx.cache.VertxAggregateCache;
import io.vertx.eventx.common.CommandHeaders;
import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;
import io.vertx.eventx.objects.*;
import io.vertx.eventx.storage.pg.mappers.AggregateSnapshotMapper;
import io.vertx.eventx.storage.pg.mappers.EventJournalMapper;
import io.vertx.eventx.storage.pg.mappers.RejectedCommandMapper;
import io.vertx.eventx.storage.pg.models.AggregateKey;
import io.vertx.mutiny.core.Vertx;
import io.vertx.eventx.Command;
import io.vertx.eventx.Behaviour;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Aggregator;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.RepositoryHandler;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.common.CustomClassLoader;
import io.vertx.mutiny.core.eventbus.DeliveryContext;

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
  private EntityConfiguration entityConfiguration;
  private RepositoryHandler repositoryHandler;
  public AggregateLogic<T> logic;
  private VertxAggregateCache<T, EntityState<T>> vertxAggregateCache = null;
  private List<BehaviourWrapper> behaviourWrappers;
  private List<AggregatorWrapper> aggregatorWrappers;

  public AggregateVerticle(
    final Class<T> aggregateClass,
    final ModuleBuilder moduleBuilder
  ) {
    this.aggregateClass = aggregateClass;
    this.moduleBuilder = moduleBuilder;
  }

  @Override
  public Uni<Void> asyncStart() {
    this.entityConfiguration = config().getJsonObject(aggregateClass.getSimpleName(), new JsonObject()).mapTo(EntityConfiguration.class);
    LOGGER.info("Starting Entity Actor " + aggregateClass.getSimpleName());
    final var injector = startInjector();
    this.aggregatorWrappers = loadAggregators(injector, aggregateClass);
    this.behaviourWrappers = loadBehaviours(injector, aggregateClass);
    if (Boolean.TRUE.equals(entityConfiguration.useCache())) {
      this.vertxAggregateCache = new VertxAggregateCache<>(
        vertx,
        aggregateClass,
        entityConfiguration.aggregateCacheTtlInMinutes()
      );
    }
    this.logic = new AggregateLogic<>(
      aggregateClass,
      aggregatorWrappers,
      behaviourWrappers,
      entityConfiguration,
      new Repository<>(EventJournalMapper.INSTANCE, repositoryHandler),
      Boolean.TRUE.equals(entityConfiguration.snapshots()) ? new Repository<>(AggregateSnapshotMapper.INSTANCE, repositoryHandler) : null,
      new Repository<>(RejectedCommandMapper.INSTANCE, repositoryHandler),
      vertxAggregateCache
    );
    return AggregateChannel.registerCommandConsumer(
      vertx,
      aggregateClass,
      this.deploymentID(),
      jsonMessage -> {
        LOGGER.info("Incoming command " + jsonMessage.body().encodePrettily());
        final var responseUni = switch (Action.valueOf(jsonMessage.headers().get(ACTION))) {
          case LOAD -> logic.load(jsonMessage.body().mapTo(AggregateKey.class));
          case COMMAND -> logic.process(jsonMessage.headers().get(CLASS_NAME), jsonMessage.body());
          case COMPOSITE_COMMAND -> logic.process(jsonMessage.body().mapTo(CompositeCommandWrapper.class));
        };
        responseUni.subscribe()
          .with(
            jsonMessage::reply,
            throwable -> {
              if (throwable instanceof EventXException vertxServiceException) {
                jsonMessage.fail(vertxServiceException.error().errorCode(), JsonObject.mapFrom(vertxServiceException.error()).encodePrettily());
              } else {
                LOGGER.error("Unexpected exception raised -> " + jsonMessage.body(), throwable);
                jsonMessage.fail(500, JsonObject.mapFrom(new EventXError(throwable.getMessage(), throwable.getLocalizedMessage(), 500)).encode());
              }
            }
          );
      }
    );
  }

  private void addContextualData(DeliveryContext<Object> event) {
    final var commandID = event.message().headers().get(CommandHeaders.COMMAND_ID);
    final var tenantID = event.message().headers().get(CommandHeaders.TENANT_ID);
    if (commandID != null) {
      ContextualData.put(CommandHeaders.COMMAND_ID, commandID);
    }
    if (tenantID != null) {
      ContextualData.put(CommandHeaders.TENANT_ID, tenantID);
    }
    ContextualData.put("aggregate", aggregateClass.getSimpleName());
    event.next();
  }

  private Injector startInjector() {
    this.repositoryHandler = RepositoryHandler.leasePool(config(), vertx, aggregateClass);
    moduleBuilder.bind(RepositoryHandler.class).toInstance(repositoryHandler);
    moduleBuilder.bind(Vertx.class).toInstance(vertx);
    moduleBuilder.bind(JsonObject.class).toInstance(config());
    moduleBuilder.bind(EntityConfiguration.class).toInstance(entityConfiguration);
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
    AggregateChannel.killActor(vertx, aggregateClass, this.deploymentID());
    LOGGER.info("[deploymentIDs:" + vertx.deploymentIDs() + "]");
    LOGGER.info("[contextID:" + context.deploymentID() + "]");
    return repositoryHandler.close();
  }


}
