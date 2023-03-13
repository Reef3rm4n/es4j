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

  public Uni<JsonObject> loadAggregate(AggregatePlainKey aggregateRecordKey) {
    LOGGER.debug("Loading aggregate locally  -> " + aggregateRecordKey);
    return loadAggregate(aggregateRecordKey.aggregateId(), aggregateRecordKey.tenantId())
      .map(JsonObject::mapFrom);
  }

  public Uni<JsonObject> process(String commandClass, JsonObject jsonCommand) {
    LOGGER.debug("Processing " + commandClass + " -> " + jsonCommand.encodePrettily());
    final var command = parseCommand(commandClass, jsonCommand);
    return loadAggregate(command.aggregateId(), command.headers().tenantId())
      .flatMap(aggregateState -> processCommand(aggregateState, command)
        .onFailure(Conflict.class)
        .recoverWithUni(
          () -> playFromLastJournalOffset(aggregateState)
            .flatMap(reconstructedState -> processCommand(reconstructedState, command))
        )
        .onFailure().invoke(throwable -> logRejectedCommand(throwable, command))
      )
      .map(JsonObject::mapFrom);
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

  private Uni<AggregateState<T>> loadAggregate(String aggregateId, String tenant) {
    LOGGER.debug("Loading aggregateId[" + aggregateId + "::" + tenant + "]");
    AggregateState<T> state = null;
    if (infrastructure.cache() != null) {
      state = infrastructure.cache().get(new AggregateKey<>(aggregateClass, aggregateId, tenant));
    }
    if (state == null) {
      return playFromLastJournalOffset(new AggregateState<>(aggregateClass));
    } else {
      return Uni.createFrom().item(state);
    }
  }

  private Uni<AggregateState<T>> playFromLastJournalOffset(AggregateState<T> state) {
    LOGGER.debug("Playing from last journal offset");
    return infrastructure.eventStore().stream(
        streamInstruction(state),
        infraEvent -> applyEvent(state, infraEvent)
      )
      .map(avoid -> cacheState(state));
  }

  private AggregateEventStream<T> streamInstruction(AggregateState<T> state) {
    return new AggregateEventStream<>(
      state.aggregateClass(),
      state.state().aggregateId(),
      state.state().tenantID(),
      state.currentVersion(),
      state.snapshotOffset()
    );
  }

  private void aggregateEvents(final AggregateState<T> state, final List<Event> events) {
    events.stream()
      .sorted(Comparator.comparingLong(Event::eventVersion))
      .forEachOrdered(event -> {
          LOGGER.info("Aggregating event -> " + event.eventClass());
          final var parsedEvent = EventParser.getEvent(event.eventClass(), event.event());
          final var isSnapshot = parsedEvent.getClass().isAssignableFrom(SnapshotEvent.class);
          if (isSnapshot) {
            applySnapshot(state, event, parsedEvent);
          } else {
            applyEvent(state, event, parsedEvent);
          }
          state
            .addKnownCommand(event.commandId())
            .setCurrentVersion(event.eventVersion());
        }
      );

  }

  private void applyEvent(AggregateState<T> state, Event event, io.vertx.eventx.Event parsedEvent) {
    final var newState = aggregateEvent(state.state(), parsedEvent);
    LOGGER.info("New aggregate state -> " + newState);
    if (state.knownCommands().stream().noneMatch(txId -> txId.equals(event.commandId()))) {
      state.knownCommands().add(event.commandId());
    }
    state.setState(newState);
  }

  private void applySnapshot(AggregateState<T> state, Event event, io.vertx.eventx.Event parsedEvent) {
    LOGGER.debug("Applying snapshot " + JsonObject.mapFrom(event).encodePrettily());
    if (parsedEvent.schemaVersion() != state.state().schemaVersion()) {
      state.setState(state.aggregateClass().cast(state.state().transformSnapshot(parsedEvent.schemaVersion(), event.event())));
    } else {
      state.setState(JsonObject.mapFrom(((SnapshotEvent) parsedEvent).state()).mapTo(state.aggregateClass()));
    }
    state.setSnapshotOffset(event.journalOffset());
  }

  private void applyEvent(final AggregateState<T> state, final Event event) {
    LOGGER.info("Aggregating event -> " + event.eventClass());
    final var parsedEvent = EventParser.getEvent(event.eventClass(), event.event());
    final var newState = aggregateEvent(state.state(), parsedEvent);
    LOGGER.info("New aggregate state -> " + newState);
    if (state.knownCommands().stream().noneMatch(txId -> txId.equals(event.commandId()))) {
      state.knownCommands().add(event.commandId());
    }
    final var isSnapshot = parsedEvent.getClass().isAssignableFrom(SnapshotEvent.class);
    state.setState(newState)
      .setSnapshotOffset(isSnapshot ? event.journalOffset() : state.snapshotOffset())
      .addKnownCommand(event.commandId())
      .setCurrentVersion(event.eventVersion());
  }

  private List<Event> applyBehaviour(final AggregateState<T> state, final Command finalCommand) {
    final var currentVersion = state.currentVersion() == null ? 0 : state.currentVersion();
    final var events = applyBehaviour(state.state(), finalCommand);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Events created -> " + new JsonArray(events).encodePrettily());
    }
    final var array = events.toArray(new io.vertx.eventx.Event[0]);
    final var resultingEvents = transformEvents(state, finalCommand, currentVersion, array);
    addOptionalSnapshot(state, finalCommand, resultingEvents);
    return resultingEvents;
  }

  private ArrayList<Event> transformEvents(AggregateState<T> state, Command finalCommand, long currentVersion, io.vertx.eventx.Event[] array) {
    return new ArrayList<>(IntStream.range(1, array.length + 1)
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
      ).toList()
    );
  }

  private void addOptionalSnapshot(AggregateState<T> state, Command finalCommand, ArrayList<Event> resultingEvents) {
    state.state().snapshotEvery().ifPresent(
      snapshotEvery -> {
        final var snapshot = snapshotEvery <= Math.floorMod(state.currentVersion(), snapshotEvery);
        if (snapshot) {
          LOGGER.debug("Appending a snapshot to the stream");
          state.setCurrentVersion(state.currentVersion() + 1);
          resultingEvents.add(new Event(
              aggregateClass.getName(),
              finalCommand.aggregateId(),
              SnapshotEvent.class.getName(),
              state.currentVersion(),
              JsonObject.mapFrom(new SnapshotEvent(
                  JsonObject.mapFrom(state.state()).getMap(),
                  state.knownCommands().stream().toList(),
                  state.snapshotOffset(),
                  state.currentVersion()
                )
              ),
              finalCommand.headers().tenantId(),
              finalCommand.headers().commandID(),
              List.of("eventx-snapshot"),
              state.state().schemaVersion()
            )
          );
        }
      }
    );
  }


  private <C extends Command> Uni<AggregateState<T>> processCommand(final AggregateState<T> state, final C command) {
    checkCommandId(state, command);
    final var events = applyBehaviour(state, command);
    aggregateEvents(state, events);
    return appendEvents(state, events).map(avoid -> cacheState(state));
  }

  private <C extends Command> void checkCommandId(AggregateState<T> state, C command) {
    if (state.knownCommands() != null && !state.knownCommands().isEmpty()) {
      state.knownCommands().stream().filter(txId -> txId.equals(command.headers().commandID()))
        .findAny()
        .ifPresent(duplicatedCommand -> {
            throw new CommandRejected(new EventxError("Command was already processed", "commandId was marked as known by the aggregate", 400));
          }
        );
    }
  }

  private AggregateState<T> cacheState(AggregateState<T> state) {
    infrastructure.cache().put(
      new AggregateKey<>(
        aggregateClass,
        state.state().aggregateId(),
        state.state().tenantID()
      ),
      state);
    return state;
  }

  private Uni<Void> appendEvents(AggregateState<T> state, List<Event> events) {
    return infrastructure.eventStore().append(
      new AppendInstruction<>(
        aggregateClass,
        state.state().aggregateId(),
        state.state().tenantID(),
        events
      )
    );
  }

  private void logRejectedCommand(final Throwable throwable, final Command command) {
    LOGGER.error("Command rejected " + JsonObject.mapFrom(command).encodePrettily(), throwable);
  }


  private Command parseCommand(final String commandType, final JsonObject jsonCommand) {
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
