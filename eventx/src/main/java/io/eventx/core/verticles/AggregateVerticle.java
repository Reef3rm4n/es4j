package io.eventx.core.verticles;


import io.eventx.core.objects.*;
import io.eventx.infrastructure.bus.ProjectionService;
import io.eventx.infrastructure.misc.EventxClassLoader;
import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.mutiny.tuples.Tuple2;
import io.eventx.Event;
import io.eventx.core.CommandHandler;
import io.eventx.core.exceptions.EventxException;
import io.eventx.infrastructure.Infrastructure;
import io.eventx.infrastructure.bus.AggregateBus;
import io.eventx.launcher.EventxMain;
import io.vertx.core.Promise;
import io.eventx.Command;
import io.eventx.Behaviour;
import io.eventx.Aggregate;
import io.eventx.Aggregator;
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
import java.util.concurrent.CountDownLatch;

import static io.eventx.core.CommandHandler.camelToKebab;

public class AggregateVerticle<T extends Aggregate> extends AbstractVerticle implements Resource {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateVerticle.class);

  public static final String ACTION = "action";
  private final Class<T> aggregateClass;
  private final String deploymentID;
  private AggregateConfiguration aggregateConfiguration;
  private CommandHandler<T> commandHandler;
  private List<BehaviourWrap> behaviourWraps;
  private List<AggregatorWrap> aggregatorWraps;
  private Infrastructure infrastructure;
  private ProjectionService projectionService;

  public AggregateVerticle(
    final Class<T> aggregateClass,
    final String deploymentID
  ) {
    this.aggregateClass = aggregateClass;
    this.deploymentID = deploymentID;
    org.crac.Core.getGlobalContext().register(this);
  }

  @Override
  public Uni<Void> asyncStart() {
    config().put("schema", camelToKebab(aggregateClass.getSimpleName()));
    this.aggregateConfiguration = config().getJsonObject("aggregate-configuration", new JsonObject()).mapTo(AggregateConfiguration.class);
    LOGGER.info("Event.x starting {}::{}", aggregateClass.getSimpleName(), this.deploymentID);
    this.aggregatorWraps = loadAggregators(aggregateClass);
    this.behaviourWraps = loadBehaviours(aggregateClass);
    this.infrastructure = new Infrastructure(
      EventxClassLoader.loadCache(),
      EventxClassLoader.loadEventStore(),
      Optional.empty(),
      EventxClassLoader.loadOffsetStore()
    );
    infrastructure.start(aggregateClass, vertx, config());
    vertx.eventBus().addInboundInterceptor(this::addContextualData);
    this.commandHandler = new CommandHandler<>(
      vertx,
      aggregateClass,
      aggregatorWraps,
      behaviourWraps,
      infrastructure,
      aggregateConfiguration
    );
    this.projectionService = new ProjectionService(
      infrastructure.offsetStore(),
      infrastructure.eventStore(),
      aggregateClass
    );
    // todo as behaviours are loaded also register consumer in order to avoid unsafe operation ?
    return projectionService.register(vertx).flatMap(avoid -> registerAggregateBus());
  }

  private Uni<Void> registerAggregateBus() {
    return Multi.createFrom().iterable(behaviourWraps)
      .onItem().transformToUniAndMerge(
        cmdBehaviour -> AggregateBus.registerCommandConsumer(
          vertx,
          aggregateClass,
          deploymentID,
          message -> messageHandler(cmdBehaviour, message),
          cmdBehaviour.commandClass()
        )
      )
      .collect().asList()
      .flatMap(avoid -> AggregateBus.waitForRegistration(deploymentID, aggregateClass))
      .replaceWithVoid();
  }

  private <A extends Aggregate, C extends Command> void messageHandler(BehaviourWrap<A, C> cmdBehaviour, Message<JsonObject> message) {
    // todo add command name to contextual data
    commandHandler.process(parseCommand(cmdBehaviour.commandClass(), message))
      .subscribe()
      .with(
        message::reply,
        throwable -> {
          if (throwable instanceof EventxException vertxServiceException) {
            message.fail(vertxServiceException.error().externalErrorCode(), JsonObject.mapFrom(vertxServiceException.error()).encodePrettily());
          } else {
            LOGGER.error("Unexpected exception raised", throwable);
            message.fail(500, JsonObject.mapFrom(new EventxError(throwable.getMessage(), throwable.getLocalizedMessage(), 500)).encode());
          }
        }
      );
  }

  private static Command parseCommand(Class<? extends Command> cmdClass, Message<JsonObject> message) {
    try {
      LOGGER.debug("Incoming command {} {}", cmdClass.getName(), message.body().encodePrettily());
      return message.body().mapTo(cmdClass);
    } catch (Exception e) {
      message.fail(400, JsonObject.mapFrom(
          EventxErrorBuilder.builder()
            .cause("Invalid message body")
            .errorSource(ErrorSource.UNKNOWN)
            .hint(e.getMessage())
            .externalErrorCode(400)
            .internalCode("400L")
            .build()
        ).encode()
      );
      throw new IllegalArgumentException();
    }
  }


  private void addContextualData(DeliveryContext<Object> event) {
    ContextualData.put("AGGREGATE", aggregateClass.getSimpleName());
    event.next();
  }

  public static <T extends Aggregate> List<BehaviourWrap> loadBehaviours(Class<T> aggregateClass) {
    final var behaviours = new ArrayList<>(EventxClassLoader.loadBehaviours().stream()
      .filter(behaviour ->
        parseCommandBehaviourGenericTypes(behaviour.getClass()).getItem1().isAssignableFrom(aggregateClass))
      .map(commandBehaviour -> {
          final var genericTypes = parseCommandBehaviourGenericTypes(commandBehaviour.getClass());
          return new BehaviourWrap(commandBehaviour, aggregateClass, genericTypes.getItem2());
        }
      )
      .toList());
    if (behaviours.isEmpty()) {
      throw new IllegalStateException("Behaviours not found for " + aggregateClass.getSimpleName());
    }
    if (!EventxMain.AGGREGATE_COMMANDS.containsKey(aggregateClass)) {
      final var behavioursClass = new ArrayList<Class<? extends Command>>(behaviours.stream()
        .map(wrapper -> (Class<? extends Command>) wrapper.commandClass())
        .toList());
      behavioursClass.add(LoadAggregate.class);
      EventxMain.AGGREGATE_COMMANDS.put(
        aggregateClass,
        behavioursClass
      );
    }
    behaviours.add(new BehaviourWrap((state, command) -> List.of(), aggregateClass, LoadAggregate.class));
    return behaviours;
  }

  public static <T extends Aggregate> List<AggregatorWrap> loadAggregators(Class<T> aggregateClass) {
    final var aggregators = EventxClassLoader.loadAggregators().stream()
      .map(aggregator -> {
          final var genericTypes = parseAggregatorClass(aggregator.getClass());
          return new AggregatorWrap(aggregator, genericTypes.getItem1(), genericTypes.getItem2());
        }
      )
      .filter(behaviour -> behaviour.entityAggregateClass().isAssignableFrom(aggregateClass))
      .toList();
    if (aggregators.isEmpty()) {
      throw new IllegalStateException("Aggregators not found for " + aggregateClass.getSimpleName());
    }
    EventxMain.AGGREGATE_EVENTS.putIfAbsent(
      aggregateClass, aggregators.stream()
        .map(wrapper -> (Class<Event>) wrapper.eventClass())
        .toList()
    );
    return aggregators;
  }

  public static Tuple2<Class<? extends Aggregate>, Class<?>> parseAggregatorClass(Class<? extends Aggregator> behaviour) {
    Type[] genericInterfaces = behaviour.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException(behaviour.getName() + " should only implement Aggregator interface");
    } else if (genericInterfaces.length == 0) {
      // should not happen ever.
      throw new IllegalArgumentException(behaviour.getName() + " should implement Aggregator interface");
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
        throw new IllegalArgumentException("Unable to parse generic type", e);
      }
      return Tuple2.of(entityClass, eventClass);
    } else {
      throw new IllegalArgumentException("Invalid Interface -> " + genericInterface.getClass());
    }
  }

  public static Tuple2<Class<? extends Aggregate>, Class<? extends Command>> parseCommandBehaviourGenericTypes(Class<? extends Behaviour> behaviour) {
    Type[] genericInterfaces = behaviour.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException(behaviour.getName() + "should only implement Behaviour interface");
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException(behaviour.getName() + " should implement Behaviour interface");
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
      throw new IllegalArgumentException("Invalid interface -> " + genericInterface.getClass());
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
    Promise<Void> p = Promise.promise();
    stop(p);
    CountDownLatch latch = new CountDownLatch(1);
    p.future().onComplete(event -> latch.countDown());
    latch.await();
  }

  @Override
  public void afterRestore(Context<? extends Resource> context) throws Exception {
    Promise<Void> p = Promise.promise();
    start(p);
    CountDownLatch latch = new CountDownLatch(1);
    p.future().onComplete(event -> latch.countDown());
    latch.await();
  }

}
