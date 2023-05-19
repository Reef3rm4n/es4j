package io.vertx.eventx.core;

import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Command;
import io.vertx.eventx.core.objects.*;
import io.vertx.eventx.infrastructure.Infrastructure;
import io.vertx.eventx.infrastructure.misc.EventParser;
import io.vertx.eventx.infrastructure.models.*;
import io.vertx.eventx.core.exceptions.CommandRejected;
import io.vertx.eventx.core.exceptions.UnknownCommand;
import io.vertx.eventx.core.exceptions.UnknownEvent;
import io.vertx.eventx.infrastructure.models.Event;
import io.vertx.eventx.sql.exceptions.Conflict;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.stream.IntStream;

public class CommandHandler<T extends Aggregate> {
  private final List<BehaviourWrapper> behaviours;
  private final List<AggregatorWrapper> aggregators;
  private final Infrastructure infrastructure;
  private static final Logger LOGGER = LoggerFactory.getLogger(CommandHandler.class);
  private final Class<T> aggregateClass;
  private final Map<String, String> commandClassMap = new HashMap<>();
  private final AggregateConfiguration aggregateConfiguration;

  private final Vertx vertx;

  public CommandHandler(
    final Vertx vertx,
    final Class<T> aggregateClass,
    final List<AggregatorWrapper> aggregators,
    final List<BehaviourWrapper> behaviours,
    final Infrastructure infrastructure,
    final AggregateConfiguration aggregateConfiguration
  ) {
    this.vertx = vertx;
    this.infrastructure = infrastructure;
    this.aggregateClass = aggregateClass;
    this.aggregators = aggregators;
    this.behaviours = behaviours;
    if (behaviours.isEmpty()) {
      throw new IllegalStateException("Empty behaviours");
    }
    if (aggregators.isEmpty()) {
      throw new IllegalStateException("Empty behaviours");
    }
    populateCommandClassMap(behaviours);
    this.aggregateConfiguration = aggregateConfiguration;
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

  private Uni<JsonObject> replay(LoadAggregate loadAggregate) {
    if (Objects.nonNull(loadAggregate.dateTo()) || Objects.nonNull(loadAggregate.versionTo())) {
      return replayAndAggregate(loadAggregate).map(AggregateState::toJson);
    }
    return replayAggregateAndCache(loadAggregate.aggregateId(), loadAggregate.headers().tenantId()).map(AggregateState::toJson);
  }

  public Uni<JsonObject> process(String commandClass, JsonObject jsonCommand) {
    final var command = parseCommand(commandClass, jsonCommand);
    if (command instanceof LoadAggregate loadAggregate) {
      return replay(loadAggregate);
    }
    if (command.options().simulate()) {
      return replayAndSimulate(command);
    }
    return replayAndAppend(command);
  }

  private Uni<JsonObject> replayAndAppend(Command command) {
    return replayAggregateAndCache(command.aggregateId(), command.headers().tenantId())
      .flatMap(aggregateState -> processCommand(aggregateState, command)
        .onFailure(Conflict.class).recoverWithUni(
          () -> playFromLastJournalOffset(command.aggregateId(), command.headers().tenantId(), aggregateState)
            .flatMap(reconstructedState -> processCommand(reconstructedState, command))
            .onFailure(Conflict.class).retry().atMost(5)
        )
        .onFailure().invoke(throwable -> logRejectedCommand(throwable, command))
      )
      .map(AggregateState::toJson);
  }

  private Uni<JsonObject> replayAndSimulate(Command command) {
    return replayAggregateAndCache(command.aggregateId(), command.headers().tenantId())
      .map(aggregateState -> {
          checkCommandId(aggregateState, command);
          final var events = applyCommandBehaviour(aggregateState, command);
          aggregateEvents(aggregateState, events);
          return aggregateState.toJson();
        }
      );
  }

  private T aggregateEvent(T aggregateState, final io.vertx.eventx.Event event) {
    io.vertx.eventx.Event finalEvent = event;
    final var aggregator = Objects.requireNonNullElse(customAggregator(event), defaultAggregator(event));
    LOGGER.debug("Applying {} schema versionTo {} ", aggregator.delegate().getClass().getSimpleName(), aggregator.delegate().currentSchemaVersion());
    if (aggregator.delegate().currentSchemaVersion() != event.schemaVersion()) {
      LOGGER.debug("Schema versionTo mismatch, migrating event {} {} ", event.getClass().getName(), JsonObject.mapFrom(event).encodePrettily());
      finalEvent = aggregator.delegate().transformFrom(event.schemaVersion(), JsonObject.mapFrom(event));
    }
    final var newAggregateState = (T) aggregator.delegate().apply(aggregateState, finalEvent);
    LOGGER.debug("State after aggregation {}", newAggregateState);
    return newAggregateState;
  }

  private AggregatorWrapper defaultAggregator(io.vertx.eventx.Event event) {
    return aggregators.stream()
      .filter(behaviour -> Objects.equals(behaviour.delegate().tenantId(), "default"))
      .filter(aggregator -> aggregator.eventClass().getName().equals(event.getClass().getName()))
      .findFirst()
      .orElseThrow(() -> UnknownEvent.unknown(event.getClass()));
  }

  private AggregatorWrapper customAggregator(io.vertx.eventx.Event event) {
    return aggregators.stream()
      .filter(behaviour -> !Objects.equals(behaviour.delegate().tenantId(), "default"))
      .filter(aggregator -> aggregator.eventClass().getName().equals(event.getClass().getName()))
      .findFirst()
      .orElse(null);
  }

  private List<io.vertx.eventx.Event> applyCommandBehaviour(final T aggregateState, final Command command) {
    final var behaviour = Objects.requireNonNullElse(customBehaviour(command), defaultBehaviour(command));
    behaviour.delegate().requiredRoles();
    LOGGER.debug("Applying {} {} ", behaviour.delegate().getClass().getSimpleName(), JsonObject.mapFrom(command));
    final var events = behaviour.process(aggregateState, command);
    LOGGER.debug("{} behaviour produced {}", behaviour.delegate().getClass().getSimpleName(), new JsonArray(events).encodePrettily());
    return events;
  }


  private BehaviourWrapper defaultBehaviour(Command command) {
    return behaviours.stream()
      .filter(behaviour -> Objects.equals(behaviour.delegate().tenantID(), "default"))
      .filter(behaviour -> behaviour.commandClass().getName().equals(command.getClass().getName()))
      .findFirst()
      .orElseThrow(() -> UnknownCommand.unknown(command.getClass()));
  }

  private BehaviourWrapper customBehaviour(Command command) {
    return behaviours.stream()
      .filter(behaviour -> !Objects.equals(behaviour.delegate().tenantID(), "default"))
      .filter(behaviour -> behaviour.commandClass().getName().equals(command.getClass().getName()))
      .findFirst()
      .orElse(null);
  }

  private Uni<AggregateState<T>> replayAggregateAndCache(String aggregateId, String tenant) {
    AggregateState<T> state = null;
    if (infrastructure.cache() != null) {
      state = infrastructure.cache().get(new AggregateKey<>(aggregateClass, aggregateId, tenant));
    }
    if (state == null) {
      return playFromLastJournalOffset(aggregateId, tenant, new AggregateState<>(aggregateClass));
    } else {
      return Uni.createFrom().item(state);
    }
  }

  private Uni<AggregateState<T>> playFromLastJournalOffset(String aggregateId, String tenant, AggregateState<T> state) {
    final var instruction = streamInstruction(aggregateId, tenant, state);
    LOGGER.debug("Playing aggregate stream with instruction {}", instruction.toJson().encodePrettily());
    return infrastructure.eventStore().fetch(instruction)
      .map(events -> {
          events.forEach(ev -> applyEvent(state, ev));
          return cacheState(state);
        }
      );
  }

  private Uni<AggregateState<T>> replayAndAggregate(LoadAggregate loadAggregate) {
    final var state = new AggregateState<>(aggregateClass);
    final var instruction = eventStreamInstruction(loadAggregate);
    LOGGER.debug("Playing aggregate stream with instruction {}", instruction);
    return infrastructure.eventStore().stream(instruction, event -> applyEvent(state, event))
      .replaceWith(state);
  }

  private EventStream eventStreamInstruction(LoadAggregate loadAggregate) {
    return EventStreamBuilder
      .builder()
      .aggregates(List.of(aggregateClass))
      .aggregateIds(List.of(loadAggregate.aggregateId()))
      .tenantId(loadAggregate.headers().tenantId())
      .offset(0L)
      .to(loadAggregate.dateTo())
      .versionTo(loadAggregate.versionTo())
      .build();
  }

  private AggregateEventStream<T> streamInstruction(String aggregateId, String tenant, AggregateState<T> state) {
    // todo use aggregate configuration to limit the size of the instruction
    //  the log from the last compression can only be as long as the compression policy
    return new AggregateEventStream<>(
      state.aggregateClass(),
      aggregateId,
      tenant,
      state.currentVersion(),
      state.currentJournalOffset(),
      SnapshotEvent.class,
      aggregateConfiguration.snapshotEvery()
    );
  }

  private void aggregateEvents(final AggregateState<T> state, final List<Event> events) {
    events.stream()
      .sorted(Comparator.comparingLong(Event::eventVersion))
      .forEachOrdered(event -> {
          final var parsedEvent = EventParser.getEvent(event.eventClass(), event.event());
          final var isSnapshot = parsedEvent.getClass().isAssignableFrom(SnapshotEvent.class);
          if (isSnapshot) {
            LOGGER.debug("Aggregating snapshot {}", event.event().encodePrettily());
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
    if (state.knownCommands().stream().noneMatch(txId -> txId.equals(event.commandId()))) {
      LOGGER.debug("Acknowledging command {}", event.commandId());
      state.knownCommands().add(event.commandId());
    }
    state.setState(newState);
  }

  private void applySnapshot(AggregateState<T> state, Event event, io.vertx.eventx.Event parsedEvent) {
    LOGGER.debug("Applying snapshot {}", JsonObject.mapFrom(event).encodePrettily());
    if (parsedEvent.schemaVersion() != state.state().schemaVersion()) {
      LOGGER.debug("Aggregate schema versionTo mismatch, migrating schema from {} to {}", parsedEvent.schemaVersion(), state.state().schemaVersion());
      state.setState(state.aggregateClass().cast(state.state().transformSnapshot(parsedEvent.schemaVersion(), event.event())));
    } else {
      LOGGER.debug("Applying snapshot with schema versionTo {} to aggregate with schema versionTo {}", parsedEvent.schemaVersion(), state.state().schemaVersion());
      state.setState(JsonObject.mapFrom(((SnapshotEvent) parsedEvent).state()).mapTo(state.aggregateClass()));
    }
  }

  private void applyEvent(final AggregateState<T> state, final Event event) {
    LOGGER.info("Aggregating event {} ", event.eventClass());
    final var parsedEvent = EventParser.getEvent(event.eventClass(), event.event());
    if (parsedEvent.getClass().isAssignableFrom(SnapshotEvent.class)) {
      final var snapshot = (SnapshotEvent) parsedEvent;
      state.knownCommands().clear();
      state.setState(JsonObject.mapFrom(snapshot.state()).mapTo(state.aggregateClass()))
        .addKnownCommands(snapshot.knownCommands())
        .setCurrentVersion(event.eventVersion())
        .setCurrentJournalOffset(event.journalOffset());
    } else {
      final var newState = aggregateEvent(state.state(), parsedEvent);
      LOGGER.debug("State after aggregation {} ", JsonObject.mapFrom(newState).encodePrettily());
      state.setState(newState)
        .addKnownCommand(event.commandId())
        .setCurrentJournalOffset(event.journalOffset())
        .setCurrentVersion(event.eventVersion());
    }
  }

  private List<Event> applyCommandBehaviour(final AggregateState<T> state, final Command finalCommand) {
    final var events = applyCommandBehaviour(state.state(), finalCommand);
    final var array = events.toArray(new io.vertx.eventx.Event[0]);
    final var resultingEvents = transformEvents(state, finalCommand, array);
    addOptionalSnapshot(state, finalCommand, resultingEvents);
    return resultingEvents;
  }

  public static <X extends Aggregate> ArrayList<Event> transformEvents(AggregateState<X> state, Command finalCommand, io.vertx.eventx.Event[] array) {
    final var currentVersion = state.currentVersion() == null ? 0 : state.currentVersion();
    return new ArrayList<>(IntStream.range(1, array.length + 1)
      .mapToObj(index -> {
          final var ev = array[index - 1];
          final var eventVersion = currentVersion + index;
          return new Event(
            finalCommand.aggregateId(),
            ev.getClass().getName(),
            eventVersion,
            JsonObject.mapFrom(ev),
            finalCommand.headers().tenantId(),
            finalCommand.headers().commandId(),
            ev.tags(),
            ev.schemaVersion()
          );
        }
      ).toList()
    );
  }

  private void addOptionalSnapshot(AggregateState<T> state, Command finalCommand, ArrayList<Event> resultingEvents) {
    if (state.state() != null) {
      state.state().snapshotEvery().ifPresent(
        snapshotEvery -> {
          final var snapshot = snapshotEvery <= Math.floorMod(state.currentVersion(), snapshotEvery);
          if (snapshot) {
            state.setCurrentVersion(state.currentVersion() + 1);
            final var snapshotEvent = new SnapshotEvent(
              JsonObject.mapFrom(state.state()).getMap(),
              state.knownCommands().stream().toList(),
              state.currentVersion()
            );
            LOGGER.debug("Appending a snapshot {}", JsonObject.mapFrom(snapshotEvent).encodePrettily());
            resultingEvents.add(new Event(
              finalCommand.aggregateId(),
              SnapshotEvent.class.getName(),
              state.currentVersion(),
              JsonObject.mapFrom(
                snapshotEvent),
              finalCommand.headers().tenantId(),
              finalCommand.headers().commandId(),
              null,
              state.state().schemaVersion()
            ));
          }
        }
      );
    }
  }


  private <C extends Command> Uni<AggregateState<T>> processCommand(final AggregateState<T> state, final C command) {
    checkCommandId(state, command);
    final var events = applyCommandBehaviour(state, command);
    aggregateEvents(state, events);
    return appendEvents(state, events)
      .map(avoid -> cacheState(state))
      .invoke(avoid -> publishStateToEventBus(state));
  }

  private void publishStateToEventBus(AggregateState<T> state) {
    final var address = EventbusStateProjection.subscriptionAddress(state.aggregateClass(), state.state().tenantID());
    try {
      vertx.eventBus().publish(address, state.toJson(), new DeliveryOptions().setLocalOnly(false).setTracingPolicy(TracingPolicy.ALWAYS));
    } catch (Exception exception) {
      LOGGER.error("Unable to publish state update for {}::{} on address {}", state.aggregateClass().getSimpleName(), state.state().aggregateId(), address);
    }
    LOGGER.debug("State update published for {}::{} to address {}", state.aggregateClass().getSimpleName(), state.state().aggregateId(), address);
  }

  private <C extends Command> void checkCommandId(AggregateState<T> state, C command) {
    if (state.knownCommands() != null && !state.knownCommands().isEmpty()) {
      state.knownCommands().stream().filter(txId -> txId.equals(command.headers().commandId()))
        .findAny()
        .ifPresent(duplicatedCommand -> {
            throw new CommandRejected(new EventxError(
              "Command was already processed",
              "Command was already processed by aggregate",
              400
            )
            );
          }
        );
    }
  }

  private AggregateState<T> cacheState(AggregateState<T> state) {
    if (state.state() != null) {
      infrastructure.cache().put(
        new AggregateKey<>(
          aggregateClass,
          state.state().aggregateId(),
          state.state().tenantID()
        ),
        state);
    }
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
    LOGGER.error("{} command rejected {}", command.getClass().getName(), JsonObject.mapFrom(command).encodePrettily(), throwable);
  }


  private Command parseCommand(final String commandType, final JsonObject jsonCommand) {
    try {
      final var clazz = Class.forName(Objects.requireNonNullElse(commandClassMap.get(commandType), commandType));
      final var object = jsonCommand.mapTo(clazz);
      if (object instanceof Command command) {
        return command;
      } else {
        throw new CommandRejected(new EventxError(
          ErrorSource.LOGIC,
          JsonObject.mapFrom(object).encode(),
          "Command is not an instance of AggregateCommand",
          "Unable to parse command " + commandType + jsonCommand.encodePrettily(),
          "command.parser",
          500
        ));
      }
    } catch (Exception e) {
      LOGGER.error("Error casting to {} {}", commandType, jsonCommand.encodePrettily(), e);
      throw new CommandRejected(new EventxError(
        ErrorSource.LOGIC,
        this.getClass().getName(),
        "Unable to parse command",
        "Unable to parse command " + commandType + jsonCommand.encodePrettily(),
        "command.parser",
        500
      )
      );
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

  public static String camelToKebab(String str) {
    // Regular Expression
    String regex = "([a-z])([A-Z]+)";

    // Replacement string
    String replacement = "$1-$2";

    // Replace the given regex
    // with replacement string
    // and convert it to lower case.
    str = str.replaceAll(regex, replacement).toLowerCase();

    // return string
    return str;
  }

}
