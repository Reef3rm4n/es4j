package io.eventx.core;

import io.eventx.Aggregate;
import io.eventx.Command;
import io.eventx.Event;
import io.eventx.core.objects.*;
import io.eventx.infrastructure.Infrastructure;
import io.eventx.infrastructure.models.*;
import io.eventx.sql.exceptions.Conflict;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.tracing.TracingPolicy;
import io.eventx.infrastructure.misc.EventParser;
import io.eventx.core.exceptions.CommandRejected;
import io.eventx.core.exceptions.UnknownCommand;
import io.eventx.core.exceptions.UnknownEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.stream.IntStream;

public class CommandHandler<T extends Aggregate> {
  private final List<BehaviourWrap> behaviours;
  private final List<AggregatorWrap> aggregators;
  private final Infrastructure infrastructure;
  private static final Logger LOGGER = LoggerFactory.getLogger(CommandHandler.class);
  private final Class<T> aggregateClass;
  private final AggregateConfiguration aggregateConfiguration;
  private final Vertx vertx;

  public CommandHandler(
    final Vertx vertx,
    final Class<T> aggregateClass,
    final List<AggregatorWrap> aggregatorWraps,
    final List<BehaviourWrap> behaviourWraps,
    final Infrastructure infrastructure,
    final AggregateConfiguration aggregateConfiguration
  ) {
    this.vertx = vertx;
    this.infrastructure = infrastructure;
    this.aggregateClass = aggregateClass;
    this.aggregators = aggregatorWraps;
    this.behaviours = behaviourWraps;
    if (behaviourWraps.isEmpty()) {
      throw new IllegalStateException("Empty behaviours");
    }
    if (aggregatorWraps.isEmpty()) {
      throw new IllegalStateException("Empty behaviours");
    }
    this.aggregateConfiguration = aggregateConfiguration;
  }

  private Uni<JsonObject> replay(LoadAggregate loadAggregate) {
    if (Objects.nonNull(loadAggregate.dateTo()) || Objects.nonNull(loadAggregate.versionTo())) {
      return replayAndAggregate(loadAggregate).map(AggregateState::toJson);
    }
    return replayAggregateAndCache(loadAggregate.aggregateId(), loadAggregate.tenant()).map(AggregateState::toJson);
  }

  public Uni<JsonObject> process(Command command) {
    if (command instanceof LoadAggregate loadAggregate) {
      return replay(loadAggregate);
    }
    if (command.options().simulate()) {
      return replayAndSimulate(command);
    }
    return replayAndAppend(command);
  }


  private Uni<JsonObject> replayAndAppend(Command command) {
    return replayAggregateAndCache(command.aggregateId(), command.tenant())
      .flatMap(aggregateState -> processCommand(aggregateState, command)
        .onFailure(Conflict.class).recoverWithUni(
          () -> playFromLastJournalOffset(command.aggregateId(), command.tenant(), aggregateState)
            .flatMap(reconstructedState -> processCommand(reconstructedState, command))
            .onFailure(Conflict.class).retry().atMost(5)
        )
        .onFailure().invoke(throwable -> logRejectedCommand(throwable, command))
      )
      .map(AggregateState::toJson);
  }

  private Uni<JsonObject> replayAndSimulate(Command command) {
    return replayAggregateAndCache(command.aggregateId(), command.tenant())
      .map(aggregateState -> {
          checkCommandId(aggregateState, command);
          final var events = applyCommandBehaviour(aggregateState, command);
          applyEvents(aggregateState, events);
          return aggregateState.toJson();
        }
      );
  }

  private T aggregateEvent(T aggregateState, final Event event) {
    Event finalEvent = event;
    final var aggregator = findAggregator(event);
    LOGGER.debug("Applying {} schema versionTo {} ", aggregator.delegate().getClass().getSimpleName(), aggregator.delegate().currentSchemaVersion());
    if (aggregator.delegate().currentSchemaVersion() != event.schemaVersion()) {
      LOGGER.debug("Schema versionTo mismatch, migrating event {} {} ", event.getClass().getName(), JsonObject.mapFrom(event).encodePrettily());
      finalEvent = aggregator.delegate().transformFrom(event.schemaVersion(), JsonObject.mapFrom(event));
    }
    final var newAggregateState = (T) aggregator.delegate().apply(aggregateState, finalEvent);
    LOGGER.debug("State after aggregation {}", newAggregateState);
    return newAggregateState;
  }

  private AggregatorWrap findAggregator(Event event) {
    return aggregators.stream()
      .filter(aggregator -> aggregator.eventClass().getName().equals(event.getClass().getName()))
      .findFirst()
      .orElseThrow(() -> UnknownEvent.unknown(event.getClass()));
  }

  private List<Event> applyCommandBehaviour(final T aggregateState, final Command command) {
    final var behaviour = findBehaviour(command);
    LOGGER.debug("Applying {} {} ", behaviour.delegate().getClass().getSimpleName(), JsonObject.mapFrom(command));
    final var events = behaviour.process(aggregateState, command);
    LOGGER.debug("{} behaviour produced {}", behaviour.delegate().getClass().getSimpleName(), new JsonArray(events).encodePrettily());
    return events;
  }


  private BehaviourWrap findBehaviour(Command command) {
    return behaviours.stream()
      .filter(behaviour -> behaviour.commandClass().getName().equals(command.getClass().getName()))
      .findFirst()
      .orElseThrow(() -> UnknownCommand.unknown(command.getClass()));
  }

  private Uni<AggregateState<T>> replayAggregateAndCache(String aggregateId, String tenant) {
    AggregateState<T> state = null;
    final var key = new AggregateKey<>(aggregateClass, aggregateId, tenant);
    if (infrastructure.cache().isPresent()) {
      LOGGER.info("Fetching from cache-store {}", key);
      state = infrastructure.cache().get().get(key);
    }
    if (state == null) {
      LOGGER.info("Fetching from event-store {}", key);
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
      .aggregateIds(List.of(loadAggregate.aggregateId()))
      .tenantId(loadAggregate.tenant())
      .offset(0L)
      .to(loadAggregate.dateTo())
      .versionTo(loadAggregate.versionTo())
      .build();
  }

  private AggregateEventStream<T> streamInstruction(String aggregateId, String tenant, AggregateState<T> state) {
    // todo use aggregate configuration to limit the size of the instruction
    //  the log from the last compression can only be as long as the compression policy
    // when policy is applied should trim and move events to secondary event-store
    return new AggregateEventStream<>(
      aggregateId,
      tenant,
      state.currentVersion(),
      state.currentJournalOffset(),
      SnapshotEvent.class,
      aggregateConfiguration.snapshotThreshold()
    );
  }

  private void applyEvents(final AggregateState<T> state, final List<io.eventx.infrastructure.models.Event> events) {
    events.stream()
      .sorted(Comparator.comparingLong(io.eventx.infrastructure.models.Event::eventVersion))
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

  private void applyEvent(AggregateState<T> state, io.eventx.infrastructure.models.Event event, Event parsedEvent) {
    final var newState = aggregateEvent(state.state(), parsedEvent);
    if (state.knownCommands().stream().noneMatch(txId -> txId.equals(event.commandId()))) {
      LOGGER.debug("Acknowledging command {}", event.commandId());
      state.knownCommands().add(event.commandId());
    }
    state.setState(newState);
  }

  private void applySnapshot(AggregateState<T> state, io.eventx.infrastructure.models.Event event, Event parsedEvent) {
    LOGGER.debug("Applying snapshot {}", JsonObject.mapFrom(event).encodePrettily());
    if (parsedEvent.schemaVersion() != state.state().schemaVersion()) {
      LOGGER.debug("Aggregate schema versionTo mismatch, migrating schema from {} to {}", parsedEvent.schemaVersion(), state.state().schemaVersion());
      state.setState(state.aggregateClass().cast(state.state().transformSnapshot(parsedEvent.schemaVersion(), event.event())));
    } else {
      LOGGER.debug("Applying snapshot with schema versionTo {} to aggregate with schema versionTo {}", parsedEvent.schemaVersion(), state.state().schemaVersion());
      state.setState(JsonObject.mapFrom(((SnapshotEvent) parsedEvent).state()).mapTo(state.aggregateClass()));
    }
  }

  private void applyEvent(final AggregateState<T> state, final io.eventx.infrastructure.models.Event event) {
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

  private List<io.eventx.infrastructure.models.Event> applyCommandBehaviour(final AggregateState<T> state, final Command finalCommand) {
    final var events = applyCommandBehaviour(state.state(), finalCommand);
    final var array = events.toArray(new Event[0]);
    final var resultingEvents = transformEvents(state, finalCommand, array);
    addOptionalSnapshot(state, finalCommand, resultingEvents);
    return resultingEvents;
  }

  public static <X extends Aggregate> ArrayList<io.eventx.infrastructure.models.Event> transformEvents(AggregateState<X> state, Command finalCommand, Event[] array) {
    final var currentVersion = state.currentVersion() == null ? 0 : state.currentVersion();
    return new ArrayList<>(IntStream.range(1, array.length + 1)
      .mapToObj(index -> {
          final var ev = array[index - 1];
          final var eventVersion = currentVersion + index;
          return new io.eventx.infrastructure.models.Event(
            finalCommand.aggregateId(),
            ev.getClass().getName(),
            eventVersion,
            JsonObject.mapFrom(ev),
            finalCommand.tenant(),
            finalCommand.uniqueId(),
            ev.tags(),
            ev.schemaVersion()
          );
        }
      ).toList()
    );
  }

  private void addOptionalSnapshot(AggregateState<T> state, Command finalCommand, ArrayList<io.eventx.infrastructure.models.Event> resultingEvents) {
    if (state.state() != null && Objects.nonNull(aggregateConfiguration.snapshotThreshold())) {
      final var shouldSnapshot = aggregateConfiguration.snapshotThreshold() <= Math.floorMod(state.currentVersion(), aggregateConfiguration.snapshotThreshold());
      if (shouldSnapshot) {
        state.setCurrentVersion(state.currentVersion() + 1);
        final var snapshotEvent = new SnapshotEvent(
          JsonObject.mapFrom(state.state()).getMap(),
          state.knownCommands().stream().toList(),
          state.currentVersion()
        );
        LOGGER.debug("Appending a snapshot {}", JsonObject.mapFrom(snapshotEvent).encodePrettily());
        resultingEvents.add(new io.eventx.infrastructure.models.Event(
          finalCommand.aggregateId(),
          SnapshotEvent.class.getName(),
          state.currentVersion(),
          JsonObject.mapFrom(
            snapshotEvent),
          finalCommand.tenant(),
          finalCommand.uniqueId(),
          List.of("system"),
          state.state().schemaVersion()
        ));
      }
    }
  }


  private <C extends Command> Uni<AggregateState<T>> processCommand(final AggregateState<T> state, final C command) {
    checkCommandId(state, command);
    final var events = applyCommandBehaviour(state, command);
    applyEvents(state, events);
    return appendEvents(state, events)
      .map(avoid -> cacheState(state))
      .invoke(avoid -> publishToEventStream(state, events))
      .invoke(avoid -> publishToStateStream(state));
  }

  private void publishToEventStream(AggregateState<T> state, List<io.eventx.infrastructure.models.Event> events) {
    events.forEach(event -> {
        final var address = EventbusLiveStreams.eventLiveStream(state.aggregateClass(), state.state().aggregateId(), state.state().tenant());
        try {
          vertx.eventBus().publish(
            address,
            JsonObject.mapFrom(event),
            new DeliveryOptions()
              .setLocalOnly(false)
              .setTracingPolicy(TracingPolicy.ALWAYS)
          );
        } catch (Exception exception) {
          LOGGER.error("Unable to publish state update for {}::{} on address {}", state.aggregateClass().getSimpleName(), state.state().aggregateId(), address);
        }
        LOGGER.debug("Event stream published for {}::{} to address {}", state.aggregateClass().getSimpleName(), state.state().aggregateId(), address);
      }
    );
  }

  private void publishToStateStream(AggregateState<T> state) {
    final var address = EventbusLiveStreams.stateLiveStream(state.aggregateClass(), state.state().aggregateId(), state.state().tenant());
    try {
      vertx.eventBus().publish(address, state.toJson(), new DeliveryOptions()
        .setLocalOnly(false)
        .setTracingPolicy(TracingPolicy.ALWAYS)
      );
    } catch (Exception exception) {
      LOGGER.error("Unable to publish state update for {}::{} on address {}", state.aggregateClass().getSimpleName(), state.state().aggregateId(), address);
    }
    LOGGER.debug("State update published for {}::{} to address {}", state.aggregateClass().getSimpleName(), state.state().aggregateId(), address);
  }

  private <C extends Command> void checkCommandId(AggregateState<T> state, C command) {
    if (state.knownCommands() != null && !state.knownCommands().isEmpty()) {
      state.knownCommands().stream().filter(txId -> txId.equals(command.uniqueId()))
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
      infrastructure.cache().ifPresent(
        cache -> cache.put(
          new AggregateKey<>(
            aggregateClass,
            state.state().aggregateId(),
            state.state().tenant()
          ),
          state
        )
      );
    }
    return state;
  }

  private Uni<Void> appendEvents(AggregateState<T> state, List<io.eventx.infrastructure.models.Event> events) {
    var startStream = Uni.createFrom().voidItem();
    if (events.stream().anyMatch(event -> event.eventVersion() == 1)) {
      startStream = infrastructure.eventStore().startStream(
        new StartStream<>(
          aggregateClass,
          state.state().aggregateId(),
          state.state().tenant()
        )
      );
    }
    return startStream.flatMap(
        avoid -> infrastructure.eventStore().append(
          new AppendInstruction<>(
            aggregateClass,
            state.state().aggregateId(),
            state.state().tenant(),
            events
          )
        )
      )
      .invoke(avoid -> dumpToSecondaryStore(state, events));
  }

  private void dumpToSecondaryStore(AggregateState<T> state, List<io.eventx.infrastructure.models.Event> events) {
    if (infrastructure.secondaryEventStore().isPresent() && events.stream().anyMatch(event -> event.eventClass().equals(SnapshotEvent.class.getName()))) {
      infrastructure.eventStore().fetch(new AggregateEventStream<>(
            state.state().aggregateId(),
            state.state().tenant(),
            0L,
            null,
            null,
            null
          )
        )
        .flatMap(eventsThatCanBeTrimmed -> {
            final var snapshotVersion = findMaxSnapshotVersion(eventsThatCanBeTrimmed);
            final var eventsToTrim = getEventsToTrim(eventsThatCanBeTrimmed, snapshotVersion);
            return infrastructure.secondaryEventStore().get().append(
              new AppendInstruction<>(
                state.aggregateClass(),
                state.state().aggregateId(),
                state.state().tenant(),
                eventsToTrim
              )
            ).replaceWith(snapshotVersion);
          }
        )
        .flatMap(maxEventVersion -> infrastructure.eventStore().trim(
            new PruneEventStream<>(
              state.aggregateClass(),
              state.state().aggregateId(),
              state.state().tenant(),
              maxEventVersion
            )
          )
        )
        .subscribe()
        .with(
          avoid -> LOGGER.info("Primary event store pruned"),
          throwable -> LOGGER.info("Unable to prune primary store", throwable)
        );
    }
  }

  private static List<io.eventx.infrastructure.models.Event> getEventsToTrim(List<io.eventx.infrastructure.models.Event> eventsThatCanBeTrimmed, Long snapshotVersion) {
    return eventsThatCanBeTrimmed.stream()
      .filter(event -> event.eventVersion() < snapshotVersion)
      .toList();
  }

  private static Long findMaxSnapshotVersion(List<io.eventx.infrastructure.models.Event> eventsThatCanBeTrimmed) {
    return eventsThatCanBeTrimmed.stream()
      .filter(ev -> ev.eventClass().equals(SnapshotEvent.class.getName()))
      .max(Comparator.comparingLong(io.eventx.infrastructure.models.Event::eventVersion))
      .map(io.eventx.infrastructure.models.Event::eventVersion)
      .orElseThrow(() -> new IllegalStateException("Snapshot not found"));
  }

  private void logRejectedCommand(final Throwable throwable, final Command command) {
    LOGGER.error("{} command rejected {}", command.getClass().getName(), JsonObject.mapFrom(command).encodePrettily(), throwable);
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
