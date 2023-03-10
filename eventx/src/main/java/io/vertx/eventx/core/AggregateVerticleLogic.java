package io.vertx.eventx.core;

import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.core.json.JsonArray;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Command;
import io.vertx.eventx.infrastructure.Infrastructure;
import io.vertx.eventx.infrastructure.misc.EventParser;
import io.vertx.eventx.infrastructure.models.*;
import io.vertx.eventx.exceptions.CommandRejected;
import io.vertx.eventx.exceptions.UnknownCommand;
import io.vertx.eventx.exceptions.UnknownEvent;
import io.vertx.eventx.objects.*;
import io.vertx.eventx.sql.exceptions.Conflict;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.IntStream;

public class AggregateVerticleLogic<T extends Aggregate> {
  private final List<BehaviourWrapper> behaviours;
  private final List<AggregatorWrapper> aggregators;
  private final Infrastructure infrastructure;
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateVerticleLogic.class);
  private final AggregateConfiguration configuration;
  private final Class<T> aggregateClass;
  private final Map<String, String> commandClassMap = new HashMap<>();

  public AggregateVerticleLogic(
    final Class<T> aggregateClass,
    final List<AggregatorWrapper> aggregators,
    final List<BehaviourWrapper> behaviours,
    final AggregateConfiguration configuration,
    final Infrastructure infrastructure
  ) {
    this.infrastructure = infrastructure;
    this.aggregateClass = aggregateClass;
    this.aggregators = aggregators;
    this.behaviours = behaviours;
    this.configuration = configuration;
    if (behaviours.isEmpty()) {
      throw new IllegalStateException("Empty behaviours");
    }
    if (aggregators.isEmpty()) {
      throw new IllegalStateException("Empty behaviours");
    }
    populateCommandClassMap(behaviours);
  }

  private void populateCommandClassMap(List<BehaviourWrapper> commandBehaviours) {
    commandBehaviours.stream().map(
      cmd -> Tuple4.of(
        cmd.commandClass().getName(),
        cmd.commandClass().getSimpleName(),
        camelToSnake(cmd.commandClass().getSimpleName()
        ),
        cmd.commandClass().getSimpleName().toLowerCase()
      )
    ).forEach(tuple -> {
        commandClassMap.put(tuple.getItem1(), tuple.getItem1());
        commandClassMap.put(tuple.getItem2(), tuple.getItem1());
        commandClassMap.put(tuple.getItem3(), tuple.getItem1());
        commandClassMap.put(tuple.getItem4(), tuple.getItem1());
      }
    );
  }

  public Uni<JsonObject> fetch(AggregatePlainKey aggregateRecordKey) {
    LOGGER.debug("Loading aggregate locally  -> " + aggregateRecordKey);
    return fetch(aggregateRecordKey.aggregateId(), aggregateRecordKey.tenantId(), ConsistencyStrategy.STRONGLY_CONSISTENT)
      .map(JsonObject::mapFrom);
  }

  public Uni<JsonObject> process(String commandClass, JsonObject jsonCommand) {
    LOGGER.debug("Processing " + commandClass + " -> " + jsonCommand.encodePrettily());
    final var command = getCommand(commandClass, jsonCommand);
    return fetch(command.aggregateId(), command.headers().tenantId(), ConsistencyStrategy.EVENTUALLY_CONSISTENT)
      .flatMap(
        aggregateState -> processCommand(aggregateState, command)
          .onFailure(Conflict.class).recoverWithUni(
            () -> ensureConsistency(aggregateState, command.aggregateId(), command.headers().tenantId())
              .flatMap(reconstructedState -> processCommand(reconstructedState, command))
          )
          .onFailure().invoke(throwable -> handleRejectedCommand(throwable, command))
      )
      .invoke(this::handleSnapshot)
      .map(state -> JsonObject.mapFrom(state.state()));
  }

  private T aggregateEvent(T aggregateState, final io.vertx.eventx.Event event) {
    AggregatorWrapper wrapper;
    io.vertx.eventx.Event finalEvent = event;
    wrapper = tenantAggregator(event);
    if (wrapper == null) {
      wrapper = defaultAggregator(event);
    }
    LOGGER.info("Applying behaviour -> " + wrapper.delegate().getClass().getSimpleName());
    if (wrapper.delegate().currentSchemaVersion() != event.schemaVersion()) {
      finalEvent = wrapper.delegate().transformFrom(event.schemaVersion(), JsonObject.mapFrom(event));
    }
    return (T) wrapper.delegate().apply(aggregateState, finalEvent);
  }

  private AggregatorWrapper defaultAggregator(io.vertx.eventx.Event event) {
    return aggregators.stream()
      .filter(behaviour -> Objects.equals(behaviour.delegate().tenantId(), "default"))
      .filter(aggregator -> aggregator.eventClass().getName().equals(event.getClass().getName()))
      .findFirst()
      .orElseThrow(() -> UnknownEvent.unknown(event.getClass()));
  }

  @Nullable
  private AggregatorWrapper tenantAggregator(io.vertx.eventx.Event event) {
    return aggregators.stream()
      .filter(behaviour -> !Objects.equals(behaviour.delegate().tenantId(), "default"))
      .filter(aggregator -> aggregator.eventClass().getName().equals(event.getClass().getName()))
      .findFirst()
      .orElse(null);
  }

  private List<io.vertx.eventx.Event> applyBehaviour(final T aggregateState, final Command command) {
    final var customBehaviour = behaviours.stream()
      .filter(behaviour -> !Objects.equals(behaviour.delegate().tenantID(), "default"))
      .filter(behaviour -> behaviour.commandClass().getName().equals(command.getClass().getName()))
      .findFirst()
      .orElse(null);
    if (customBehaviour == null) {
      final var defaultBehaviour = behaviours.stream()
        .filter(behaviour -> Objects.equals(behaviour.delegate().tenantID(), "default"))
        .filter(behaviour -> behaviour.commandClass().getName().equals(command.getClass().getName()))
        .findFirst()
        .orElseThrow(() -> UnknownCommand.unknown(command.getClass()));
      LOGGER.info("Applying command Behaviour -> " + defaultBehaviour.getClass().getSimpleName());
      return defaultBehaviour.process(aggregateState, command);
    }
    LOGGER.info("Applying command behaviour -> " + customBehaviour.getClass().getSimpleName());
    return customBehaviour.process(aggregateState, command);
  }

  private Uni<AggregateState<T>> fetch(String aggregateId, String tenant, ConsistencyStrategy consistencyStrategy) {
    LOGGER.debug("Loading aggregate " + aggregateId + " with strategy " + consistencyStrategy);
    return switch (consistencyStrategy) {
      case EVENTUALLY_CONSISTENT -> {
        AggregateState<T> state = null;
        if (infrastructure.cache() != null) {
          state = infrastructure.cache().get(new AggregateKey<>(aggregateClass, aggregateId, tenant));
        }
        if (state == null) {
          yield eventSourceAggregate(aggregateId, tenant);
        } else {
          yield Uni.createFrom().item(state);
        }
      }
      case STRONGLY_CONSISTENT -> {
        AggregateState<T> state = null;
        if (infrastructure.cache() != null) {
          state = infrastructure.cache().get(new AggregateKey<>(aggregateClass, aggregateId, tenant));
        }
        if (state == null) {
          yield eventSourceAggregate(aggregateId, tenant);
        } else {
          yield ensureConsistency(state, aggregateId, tenant);
        }
      }
    };
  }

  private Uni<AggregateState<T>> ensureConsistency(AggregateState<T> state, final String aggregateId, final String tenant) {
    if (configuration.operationMode() == OperationMode.PROD) {
      LOGGER.warn("Ensuring consistency for aggregateId -> " + aggregateId);
      return infrastructure.eventStore().stream(
          new AggregateEventStream<>(
            aggregateClass,
            aggregateId,
            tenant,
            state.currentVersion(),
            state.journalOffset()
          ),
          infraEvent -> applyEvent(state, infraEvent)
        )
        .map(avoid -> {
            infrastructure.cache().put(
              new AggregateKey<>(
                aggregateClass,
                aggregateId,
                tenant
              ),
              state
            );
            return state;
          }
        );
    } else {
      LOGGER.warn("Event stream ignored operationMode[ " + configuration.operationMode() + "]");
      return Uni.createFrom().item(state);
    }
  }

  private Uni<AggregateState<T>> eventSourceAggregate(final String aggregateId, final String tenant) {
    final var state = new AggregateState<>(aggregateClass);
    LOGGER.debug("Loading aggregate from event-store -> " + aggregateId);
    return infrastructure.eventStore().stream(
        new AggregateEventStream<>(
          aggregateClass,
          aggregateId,
          tenant,
          state.currentVersion(),
          state.journalOffset()
        )
        , event -> applyEvent(state, event)
      )
      .map(avoid -> {
          infrastructure.cache().put(new AggregateKey<>(
              aggregateClass,
              aggregateId,
              tenant
            ), state
          );
          return state;
        }
      );
  }

  private void applyEvents(final AggregateState<T> state, final List<Event> events) {
    events.stream()
      .sorted(Comparator.comparingLong(Event::eventVersion))
      .forEachOrdered(event -> {
          LOGGER.info("Aggregating event -> " + event.eventClass());
          final var newState = aggregateEvent(state.state(), EventParser.getEvent(event.eventClass(), event.event()));
          LOGGER.info("New aggregate state -> " + newState);
          if (state.knownCommands().stream().noneMatch(txId -> txId.equals(event.commandId()))) {
            state.knownCommands().add(event.commandId());
          }
          state.setState(newState)
            .setJournalOffset(event.journalOffset())
            .addKnownCommand(event.commandId())
            .setCurrentVersion(event.eventVersion());
        }
      );

  }

  private void applyEvent(final AggregateState<T> state, final Event event) {
    LOGGER.info("Aggregating event -> " + event.eventClass());
    final var newState = aggregateEvent(state.state(), EventParser.getEvent(event.eventClass(), event.event()));
    LOGGER.info("New aggregate state -> " + newState);
    if (state.knownCommands().stream().noneMatch(txId -> txId.equals(event.commandId()))) {
      state.knownCommands().add(event.commandId());
    }
    state.setState(newState)
      .setJournalOffset(event.journalOffset())
      .addKnownCommand(event.commandId())
      .setCurrentVersion(event.eventVersion());
  }


  private List<Event> applyBehaviour(final AggregateState<T> state, final Command finalCommand) {
    final var currentVersion = state.currentVersion() == null ? 0 : state.currentVersion();
    final var events = applyBehaviour(state.state(), finalCommand);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Events created -> " + new JsonArray(events).encodePrettily());
    }
    final var array = events.toArray(events.toArray(new io.vertx.eventx.Event[0]));
    return IntStream.range(1, array.length + 1)
      .mapToObj(index -> {
          final var ev = array[index - 1];
          final var eventVersion = currentVersion + index;
          state.setCurrentVersion(eventVersion);
          return new Event(
            aggregateClass.getName(),
            finalCommand.aggregateId(),
            ev.getClass().getName(),
            eventVersion,
            JsonObject.mapFrom(ev),
            finalCommand.headers().tenantId(),
            finalCommand.headers().commandID(),
            ev.tags(),
            ev.schemaVersion()
          );
        }
      ).toList();
  }


  private <C extends Command> Uni<AggregateState<T>> processCommand(final AggregateState<T> state, final C command) {
    if (state.knownCommands() != null && !state.knownCommands().isEmpty()) {
      state.knownCommands().stream().filter(txId -> txId.equals(command.headers().commandID()))
        .findAny()
        .ifPresent(duplicatedCommand -> {
            throw new CommandRejected(new EventxError("Command was already processed", "commandId was marked as known by the aggregate", 400));
          }
        );
    }
    final var events = applyBehaviour(state, command);
    if (configuration.operationMode() == OperationMode.PROD) {
      applyEvents(state, events);
      return infrastructure.eventStore().append(
          new AppendInstruction<>(
            aggregateClass,
            state.state().aggregateId(),
            state.state().tenantID(),
            events
          )
        )
        .map(
          avoid -> {
            infrastructure.cache().put(
              new AggregateKey<>(
                aggregateClass,
                state.state().aggregateId(),
                state.state().tenantID()
              ),
              state);
            return state;
          }
        );
    } else {
      LOGGER.warn("Aggregate will not be persisted to the database since persistence mode is set to -> " + configuration.operationMode());
      applyEvents(state, events);
      infrastructure.cache().put(
        new AggregateKey<>(
          aggregateClass,
          state.state().aggregateId(),
          state.state().tenantID()
        ),
        state);
      return Uni.createFrom().item(state);
    }
  }

  private void handleSnapshot(AggregateState<T> state) {
    final var snapshotFrequency = 20;
    final var shouldTakeSnapshot = Math.floorMod(state.currentVersion(), snapshotFrequency) >= snapshotFrequency;
    if (shouldTakeSnapshot) {
      if (state.currentVersion() > snapshotFrequency) {
        LOGGER.info("Adding snapshot -> " + state.state().aggregateId());
        infrastructure.snapshotStore().add(state)
          .map(avoid -> updateStateAfterSnapshot(state))
          .subscribe()
          .with(
            item -> LOGGER.info("Snapshot added -> " + item),
            throwable -> LOGGER.error("Unable to add snapshot", throwable)
          );
      } else {
        LOGGER.info("Updating snapshot -> " + state.state().aggregateId());
        infrastructure.snapshotStore().update(state)
          .map(avoid -> updateStateAfterSnapshot(state))
          .subscribe()
          .with(
            item -> LOGGER.info("Snapshot added -> " + item)
            , throwable -> LOGGER.error("Unable to add snapshot", throwable));
      }
    }
  }

  private AggregateState<T> updateStateAfterSnapshot(final AggregateState<T> state) {
    infrastructure.cache().put(
      new AggregateKey<>(
        aggregateClass,
        state.state().aggregateId(),
        state.state().tenantID()
      ),
      state
    );
    return state;
  }

  private void handleRejectedCommand(final Throwable throwable, final Command command) {
    LOGGER.error("Command rejected " + JsonObject.mapFrom(command).encodePrettily(), throwable);
  }


  private Command getCommand(final String commandType, final JsonObject jsonCommand) {
    try {
      final var clazz = Class.forName(Objects.requireNonNullElse(commandClassMap.get(commandType), commandType));
      final var object = jsonCommand.mapTo(clazz);
      if (object instanceof Command command) {
        return command;
      } else {
        throw new CommandRejected(new EventxError("Command is not an instance of AggregateCommand", JsonObject.mapFrom(object).encode(), 500));
      }
    } catch (Exception e) {
      LOGGER.error("Unable to cast command", e);
      throw new CommandRejected(new EventxError("Unable to cast class", e.getMessage(), 500));
    }
  }

  public static String camelToSnake(String str) {
    // Regular Expression
    String regex = "([a-z])([A-Z]+)";

    // Replacement string
    String replacement = "$1_$2";

    // Replace the given regex
    // with replacement string
    // and convert it to lower case.
    str = str.replaceAll(regex, replacement).toLowerCase();

    // return string
    return str;
  }

}
