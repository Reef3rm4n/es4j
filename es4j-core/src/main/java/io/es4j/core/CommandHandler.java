package io.es4j.core;

import io.es4j.Aggregate;
import io.es4j.Command;
import io.es4j.Event;
import io.es4j.core.objects.*;
import io.es4j.infrastructure.Infrastructure;
import io.es4j.infrastructure.models.*;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.tracing.TracingPolicy;
import io.es4j.infrastructure.misc.EventParser;
import io.es4j.core.exceptions.CommandRejected;
import io.es4j.core.exceptions.UnknownCommand;
import io.es4j.core.exceptions.UnknownEvent;
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
    // todo should not return aggregate state but instead void
    // todo add fire and forget command
    // todo add rejected command store
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
        .onFailure(ConcurrentAppend.class).recoverWithUni(
          () -> playFromLastSnapshot(command.aggregateId(), command.tenant(), aggregateState)
            .flatMap(reconstructedState -> processCommand(reconstructedState, command))
            .onFailure(ConcurrentAppend.class).retry().atMost(5)
        )
        .onFailure().invoke(throwable -> logRejectedCommand(throwable, command, aggregateState))
      )
      .map(AggregateState::toJson);
  }

  private Uni<JsonObject> replayAndSimulate(Command command) {
    return replayAggregateAndCache(command.aggregateId(), command.tenant())
      .map(aggregateState -> {
          checkCommandId(aggregateState, command);
          final var events = applyCommandBehaviour(aggregateState, command);
          aggregateEvents(aggregateState, events);
          return aggregateState.toJson();
        }
      );
  }

  private T aggregateEvent(T aggregateState, final Event event, Integer eventSchemaVersion) {
    Event finalEvent = event;
    final var aggregator = findAggregator(event);
    LOGGER.debug("Applying {} schema versionTo {} ", aggregator.delegate().getClass().getSimpleName(), aggregator.delegate().schemaVersion());
    if (aggregator.delegate().schemaVersion() != eventSchemaVersion) {
      LOGGER.debug("Schema versionTo mismatch, migrating event {} {} ", event.getClass().getName(), JsonObject.mapFrom(event).encodePrettily());
      finalEvent = aggregator.delegate().migrate(eventSchemaVersion, JsonObject.mapFrom(event));
    }
    final var newAggregateState = (T) aggregator.delegate().apply(aggregateState, finalEvent);
    LOGGER.debug("State after aggregation {}", newAggregateState);
    return newAggregateState;
  }

  private AggregatorWrap findAggregator(Event event) {
    return aggregators.stream()
      .filter(aggregator -> aggregator.eventClass().isAssignableFrom(event.getClass()))
      .findFirst()
      .orElseThrow(() -> UnknownEvent.unknown(event));
  }

  private AggregatorWrap findAggregator(String eventType) {
    return aggregators.stream()
      .filter(aggregator -> aggregator.delegate().eventType().equals(eventType))
      .findFirst()
      .orElseThrow();
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
      LOGGER.debug("Fetching from cache-store {}", key);
      state = infrastructure.cache().get().get(key);
    }
    if (state == null) {
      LOGGER.debug("Fetching from event-store {}", key);
      return playFromLastSnapshot(aggregateId, tenant, new AggregateState<>(aggregateClass));
    } else {
      return Uni.createFrom().item(state);
    }
  }

  private Uni<AggregateState<T>> playFromLastSnapshot(String aggregateId, String tenant, AggregateState<T> state) {
    final var instruction = streamInstruction(aggregateId, tenant, state, true);
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
    return infrastructure.eventStore().stream(instruction, event -> applyEvent(state, event)).replaceWith(state);
  }

  private EventStream eventStreamInstruction(LoadAggregate loadAggregate) {
    return EventStreamBuilder.builder()
      .aggregateIds(List.of(loadAggregate.aggregateId()))
      .tenantId(loadAggregate.tenant())
      .to(loadAggregate.dateTo())
      .versionTo(loadAggregate.versionTo())
      .build();
  }

  private AggregateEventStream<T> streamInstruction(String aggregateId, String tenant, AggregateState<T> state, Boolean replayFromSnapshot) {
    return new AggregateEventStream<>(
      aggregateId,
      tenant,
      state.currentVersion(),
      state.currentJournalOffset(),
      replayFromSnapshot,
      aggregateConfiguration.snapshotThreshold()
    );
  }

  private void aggregateEvents(final AggregateState<T> state, final List<io.es4j.infrastructure.models.Event> events) {
    events.stream()
      .sorted(Comparator.comparingLong(io.es4j.infrastructure.models.Event::eventVersion))
      .forEachOrdered(event -> {
          if (event.eventType().equals("snapshot")) {
            applySnapshot(state, event, event.event().mapTo(SnapshotEvent.class), event.schemaVersion());
          } else {
            final var aggregator = findAggregator(event.eventType());
            final var parsedEvent = EventParser.getEvent(aggregator.eventClass(), event.event());
            applyEvent(state, event, parsedEvent);
          }
          state
            .addKnownCommand(event.commandId())
            .setCurrentVersion(event.eventVersion());
        }
      );

  }

  private void applyEvent(AggregateState<T> state, io.es4j.infrastructure.models.Event event, Event parsedEvent) {
    final var newState = aggregateEvent(state.state(), parsedEvent, event.schemaVersion());
    if (state.knownCommands().stream().noneMatch(txId -> txId.equals(event.commandId()))) {
      LOGGER.debug("Acknowledging command {}", event.commandId());
      state.knownCommands().add(event.commandId());
    }
    state.setState(newState);
  }

  private void applySnapshot(AggregateState<T> state, io.es4j.infrastructure.models.Event event, Event parsedEvent, Integer eventSchemaVersion) {
    LOGGER.debug("Applying snapshot {}", JsonObject.mapFrom(event).encodePrettily());
    if (eventSchemaVersion != state.state().schemaVersion()) {
      LOGGER.debug("Aggregate schema versionTo mismatch, migrating schema from {} to {}", eventSchemaVersion, state.state().schemaVersion());
      state.setState(state.aggregateClass().cast(state.state().transformSnapshot(eventSchemaVersion, event.event())));
    } else {
      LOGGER.debug("Applying snapshot with schema versionTo {} to aggregate with schema versionTo {}", eventSchemaVersion, state.state().schemaVersion());
      state.setState(JsonObject.mapFrom(((SnapshotEvent) parsedEvent).state()).mapTo(state.aggregateClass()));
    }
  }

  private void applyEvent(final AggregateState<T> state, final io.es4j.infrastructure.models.Event event) {
    LOGGER.info("Aggregating event {} ", event);
    if (event.eventType().equals("snapshot")) {
      final var snapshot = event.event().mapTo(SnapshotEvent.class);
      state.knownCommands().clear();
      state.setState(JsonObject.mapFrom(snapshot.state()).mapTo(state.aggregateClass()))
        .addKnownCommands(snapshot.knownCommands())
        .setCurrentVersion(event.eventVersion())
        .setCurrentJournalOffset(event.journalOffset());
    } else {
      final var aggregator = findAggregator(event.eventType());
      final var parsedEvent = EventParser.getEvent(aggregator.eventClass(), event.event());
      final var newState = aggregateEvent(state.state(), parsedEvent, event.schemaVersion());
      LOGGER.debug("State after aggregation {} ", JsonObject.mapFrom(newState).encodePrettily());
      state.setState(newState)
        .addKnownCommand(event.commandId())
        .setCurrentJournalOffset(event.journalOffset())
        .setCurrentVersion(event.eventVersion());
    }
  }

  private List<io.es4j.infrastructure.models.Event> applyCommandBehaviour(final AggregateState<T> state, final Command finalCommand) {
    final var events = applyCommandBehaviour(state.state(), finalCommand);
    final var array = events.toArray(new Event[0]);
    final var resultingEvents = transformEvents(state, finalCommand, array);
    addOptionalSnapshot(state, finalCommand, resultingEvents);
    return resultingEvents;
  }

  public <X extends Aggregate> ArrayList<io.es4j.infrastructure.models.Event> transformEvents(AggregateState<X> state, Command finalCommand, Event[] array) {
    final var currentVersion = Objects.requireNonNullElse(state.currentVersion(), 0L);
    return new ArrayList<>(IntStream.range(1, array.length + 1)
      .mapToObj(index -> {
          final var ev = array[index - 1];
          final var aggregator = findAggregator(ev);
          return new io.es4j.infrastructure.models.Event(
            finalCommand.aggregateId(),
            aggregator.delegate().eventType(),
            currentVersion + index,
            JsonObject.mapFrom(ev),
            finalCommand.tenant(),
            finalCommand.uniqueId(),
            ev.tags(),
            aggregator.delegate().schemaVersion()
          );
        }
      ).toList()
    );
  }

  private void addOptionalSnapshot(AggregateState<T> state, Command finalCommand, ArrayList<io.es4j.infrastructure.models.Event> resultingEvents) {
    if (state.state() != null && Objects.nonNull(aggregateConfiguration.snapshotThreshold())) {
      final var shouldSnapshot = resultingEvents.stream().anyMatch(event -> isShouldSnapshot(aggregateConfiguration.snapshotThreshold(), event.eventVersion()));
      if (shouldSnapshot) {
        final var snapshotEvent = new SnapshotEvent(
          JsonObject.mapFrom(state.state()).getMap(),
          state.knownCommands().stream().toList(),
          state.currentVersion()
        );
        LOGGER.debug("Appending a snapshot {}", JsonObject.mapFrom(snapshotEvent).encodePrettily());
        resultingEvents.add(new io.es4j.infrastructure.models.Event(
          finalCommand.aggregateId(),
          "snapshot",
          resultingEvents.stream().map(io.es4j.infrastructure.models.Event::eventVersion).max(Long::compareTo).orElseThrow() + 1,
          JsonObject.mapFrom(
            snapshotEvent),
          finalCommand.tenant(),
          finalCommand.uniqueId(),
          List.of("system-snapshot"),
          state.state().schemaVersion()
        ));
      }
    }
  }

  public static boolean isShouldSnapshot(int snapshotThreshold, Long currentEvent) {
    return currentEvent % snapshotThreshold == 0;
  }

  private <C extends Command> Uni<AggregateState<T>> processCommand(final AggregateState<T> state, final C command) {
    checkCommandId(state, command);
    final var events = applyCommandBehaviour(state, command);
    aggregateEvents(state, events);
    return appendEvents(state, events)
      .map(avoid -> cacheState(state))
      .invoke(avoid -> publishToEventStream(state, events))
      .invoke(avoid -> publishToStateStream(state));
  }

  private void publishToEventStream(AggregateState<T> state, List<io.es4j.infrastructure.models.Event> events) {
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
            throw new CommandRejected(new Es4jError(
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

  private Uni<Void> appendEvents(AggregateState<T> state, List<io.es4j.infrastructure.models.Event> events) {
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

  private void dumpToSecondaryStore(AggregateState<T> state, List<io.es4j.infrastructure.models.Event> events) {
    if (infrastructure.secondaryEventStore().isPresent() && events.stream().anyMatch(event -> event.eventType().equals(SnapshotEvent.class.getName()))) {
      infrastructure.eventStore().fetch(new AggregateEventStream<>(
            state.state().aggregateId(),
            state.state().tenant(),
            null,
            null,
            false,
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

  private static List<io.es4j.infrastructure.models.Event> getEventsToTrim(List<io.es4j.infrastructure.models.Event> eventsThatCanBeTrimmed, Long snapshotVersion) {
    return eventsThatCanBeTrimmed.stream()
      .filter(event -> event.eventVersion() < snapshotVersion)
      .toList();
  }

  private static Long findMaxSnapshotVersion(List<io.es4j.infrastructure.models.Event> eventsThatCanBeTrimmed) {
    return eventsThatCanBeTrimmed.stream()
      .filter(ev -> ev.eventType().equals(SnapshotEvent.class.getName()))
      .max(Comparator.comparingLong(io.es4j.infrastructure.models.Event::eventVersion))
      .map(io.es4j.infrastructure.models.Event::eventVersion)
      .orElseThrow(() -> new IllegalStateException("Snapshot not found"));
  }

  private void logRejectedCommand(final Throwable throwable, final Command command, AggregateState<T> aggregateState) {
    final var json = new JsonObject();
    json.put("command-type", camelToKebab(command.getClass().getSimpleName()));
    json.put("command", JsonObject.mapFrom(command));
    json.put("aggregate", aggregateState.toJson());
    LOGGER.error("Command rejected {}", json.encodePrettily(), throwable);
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
