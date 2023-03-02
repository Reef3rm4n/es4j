package io.vertx.eventx.handlers;

import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.core.json.JsonArray;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Command;
import io.vertx.eventx.cache.VertxAggregateCache;
import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.exceptions.CommandRejected;
import io.vertx.eventx.exceptions.UnknownCommand;
import io.vertx.eventx.exceptions.UnknownEvent;
import io.vertx.eventx.objects.*;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.exceptions.Conflict;
import io.vertx.eventx.sql.exceptions.NotFound;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.EmptyQuery;
import io.vertx.eventx.sql.models.QueryOptions;
import io.vertx.eventx.storage.pg.models.*;

import java.util.*;
import java.util.stream.IntStream;

public class AggregateLogic<T extends Aggregate> {
  private final List<BehaviourWrapper> behaviours;
  // todo create an interface for the eventJournal
  // todo move this repository as an implementation of the eventJournal
  private final Repository<EventRecordKey, EventRecord, EventRecordQuery> eventJournal;
  private final Repository<AggregateKey, RejectedCommand, ?> rejectedCommands;
  // todo create an interface for the snapshots
  // todo move this repository as an implementation of the snapshot
  private final Repository<AggregateKey, AggregateSnapshot, ?> snapshots;
  // todo create an interface for the entityCache
  // todo move this cache as an implementation of the entity cache
  private final VertxAggregateCache<T, EntityState<T>> cache;
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateLogic.class);
  private final EntityConfiguration configuration;
  private final List<AggregatorWrapper> aggregators;
  private final Class<T> entityClass;
  private final Map<String, String> commandClassMap = new HashMap<>();

  public AggregateLogic(
    final Class<T> entityClass,
    final List<AggregatorWrapper> aggregators,
    final List<BehaviourWrapper> behaviours,
    final EntityConfiguration configuration,
    final Repository<EventRecordKey, EventRecord, EventRecordQuery> eventJournal,
    final Repository<AggregateKey, AggregateSnapshot, EmptyQuery> snapshots,
    final Repository<AggregateKey, RejectedCommand, ?> rejectedCommands,
    final VertxAggregateCache<T, EntityState<T>> cache
  ) {
    this.eventJournal = eventJournal;
    this.entityClass = entityClass;
    this.aggregators = aggregators;
    this.behaviours = behaviours;
    this.snapshots = snapshots;
    this.rejectedCommands = rejectedCommands;
    this.configuration = configuration;
    this.cache = cache;
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

  public Uni<JsonObject> load(AggregateKey entityAggregateKey) {
    LOGGER.debug("Loading entity locally  -> " + entityAggregateKey);
    return load(entityAggregateKey.aggregateId(), entityAggregateKey.tenantId(), ConsistencyStrategy.STRONGLY_CONSISTENT)
      .map(state -> JsonObject.mapFrom(state.aggregateState()));
  }


  public Uni<JsonObject> process(CompositeCommandWrapper command) {
    LOGGER.debug("Processing CompositeCommand -> " + command);
    final var commands = command.commands().stream()
      .map(cmd -> getCommand(cmd.commandType(), cmd.command()))
      .toList();
    if (!commands.stream().allMatch(cmd -> cmd.entityId().equals(command.entityId()))) {
      throw new CommandRejected(new EventXError("ID mismatch in composite command", "All composite commands should have the same aggregateId", 400));
    }
    return load(command.entityId(), command.headers().tenantId(), ConsistencyStrategy.EVENTUALLY_CONSISTENT)
      .flatMap(
        aggregateState -> processCommand(aggregateState, commands)
          .onFailure(Conflict.class).recoverWithUni(
            () -> ensureConsistency(aggregateState, command.entityId(), command.headers().tenantId())
              .flatMap(reconstructedState -> processCommand(reconstructedState, command))
          )
          .onFailure().invoke(throwable -> handleRejectedCommand(throwable, command))
      )
      .invoke(this::handleSnapshot)
      .map(state -> JsonObject.mapFrom(state.aggregateState()));
  }

  public Uni<JsonObject> process(String commandType, JsonObject jsonCommand) {
    LOGGER.debug("Processing " + commandType + " -> " + jsonCommand.encodePrettily());
    final var command = getCommand(commandType, jsonCommand);
    return load(command.entityId(), command.headers().tenantId(), ConsistencyStrategy.EVENTUALLY_CONSISTENT)
      .flatMap(
        aggregateState -> processCommand(aggregateState, command)
          .onFailure(Conflict.class).recoverWithUni(
            () -> ensureConsistency(aggregateState, command.entityId(), command.headers().tenantId())
              .flatMap(reconstructedState -> processCommand(reconstructedState, command))
          )
          .onFailure().invoke(throwable -> handleRejectedCommand(throwable, command))
      )
      .invoke(this::handleSnapshot)
      .map(state -> JsonObject.mapFrom(state.aggregateState()));
  }

  private T aggregateEvent(T aggregateState, final Object event) {
    final var customAggregator = aggregators.stream()
      .filter(behaviour -> !Objects.equals(behaviour.delegate().tenantId(), "default"))
      .filter(aggregator -> aggregator.eventClass().getName().equals(event.getClass().getName()))
      .findFirst()
      .orElse(null);
    if (customAggregator == null) {
      final var defaultAggregator = aggregators.stream()
        .filter(behaviour -> Objects.equals(behaviour.delegate().tenantId(), "default"))
        .filter(aggregator -> aggregator.eventClass().getName().equals(event.getClass().getName()))
        .findFirst()
        .orElseThrow(() -> UnknownEvent.unknown(event.getClass()));
      LOGGER.info("Applying behaviour -> " + defaultAggregator.getClass().getSimpleName());
      return (T) defaultAggregator.delegate().apply(aggregateState, event);
    }
    LOGGER.info("Applying behaviour -> " + customAggregator.getClass().getSimpleName());
    return (T) customAggregator.delegate().apply(aggregateState, event);
  }

  private List<Object> applyBehaviour(final T aggregateState, final Command command) {
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
      return defaultBehaviour.delegate().process(aggregateState, command);
    }
    LOGGER.info("Applying command behaviour -> " + customBehaviour.getClass().getSimpleName());
    return customBehaviour.delegate().process(aggregateState, command);
  }

  private Uni<EntityState<T>> load(String entityId, String tenant, ConsistencyStrategy consistencyStrategy) {
    LOGGER.debug("Loading entity aggregate " + entityId + " with consistency lockLevel -> " + consistencyStrategy);
    return switch (consistencyStrategy) {
      case EVENTUALLY_CONSISTENT -> {
        EntityState<T> state = null;
        if (cache != null) {
          state = cache.get(new AggregateKey(entityId, tenant));
        }
        if (state == null) {
          yield loadFromDatabase(entityId, tenant);
        } else {
          yield Uni.createFrom().item(state);
        }
      }
      case STRONGLY_CONSISTENT -> {
        EntityState<T> state = null;
        if (cache != null) {
          state = cache.get(new AggregateKey(entityId, tenant));
        }
        if (state == null) {
          yield loadFromDatabase(entityId, tenant);
        } else {
          yield ensureConsistency(state, entityId, tenant);
        }
      }
    };
  }

  private Uni<EntityState<T>> ensureConsistency(EntityState<T> state, final String entityId, final String tenant) {
    if (configuration.persistenceMode() == PersistenceMode.DATABASE) {
      LOGGER.warn("Ensuring consistency for entity -> " + entityId);
      return eventJournal.query(ensureConsistencyQuery(entityId, tenant, state))
        .flatMap(events -> {
            if (events != null && !events.isEmpty()) {
              applyEvents(state, events, null);
            }
            return cache.put(new AggregateKey(entityId, tenant), state);
          }
        )
        .onFailure(NotFound.class).recoverWithItem(state);
    } else {
      LOGGER.warn("Consistency not ensure since persistence mode is set to -> " + configuration.persistenceMode());
      return Uni.createFrom().item(state);
    }
  }

  private Uni<EntityState<T>> loadFromDatabase(final String entityId, final String tenant) {
    final var state = new EntityState<T>(
      configuration.snapshotEvery(),
      configuration.maxNumberOfCommandsForIdempotency()
    );
    if (configuration.persistenceMode() == PersistenceMode.DATABASE) {
      LOGGER.warn("Loading entity from database -> " + entityId);
      Uni<Void> snapshotUni = Uni.createFrom().voidItem();
      if (snapshots != null) {
        snapshotUni = snapshots.selectByKey(new AggregateKey(entityId, tenant))
          .map(persistedSnapshot -> loadSnapshot(state, persistedSnapshot))
          .onFailure(NotFound.class).recoverWithNull()
          .replaceWithVoid();
      }
      return snapshotUni.flatMap(avoid -> eventJournal.query(eventJournalQuery(entityId, tenant, state)))
        .flatMap(events -> {
            if (events != null && !events.isEmpty()) {
              applyEvents(state, events, null);
            }
            return cache.put(new AggregateKey(entityId, tenant), state);
          }
        )
        .onFailure(NotFound.class).recoverWithItem(state);
    } else {
      LOGGER.warn("Wont look for entity in database since persistence mode is set to -> " + configuration.persistenceMode());
      return Uni.createFrom().item(state);
    }

  }

  private EntityState<T> loadSnapshot(final EntityState<T> state, final AggregateSnapshot persistedSnapshot) {
    LOGGER.info("Loading snapshot -> " + persistedSnapshot.state().encodePrettily());
    return state.setAggregateState(persistedSnapshot.state().mapTo(entityClass))
      .setCurrentEventVersion(persistedSnapshot.eventVersion())
      .setSnapshot(persistedSnapshot)
      .setSnapshotPresent(true);
  }

  private void applyEvents(final EntityState<T> state, final List<EventRecord> events, Command command) {
    events.stream()
      .sorted(Comparator.comparingLong(EventRecord::eventVersion))
      .forEachOrdered(event -> {
          LOGGER.info("Aggregating event -> " + event.eventClass());
          final var newState = aggregateEvent(state.aggregateState(), getEvent(event.eventClass(), event.event()));
          LOGGER.info("New aggregate state -> " + newState);
          if (command != null && state.commands() != null && state.commands().stream().noneMatch(txId -> txId.equals(command.headers().commandID()))) {
            state.commands().add(command.headers().commandID());
          }
          if (state.eventsAfterSnapshot() != null) {
            state.eventsAfterSnapshot().add(event);
          }
          state.setAggregateState(newState)
            .setCurrentEventVersion(event.eventVersion())
            .setProcessedEventsAfterLastSnapshot(state.processedEventsAfterLastSnapshot() + 1);
        }
      );
  }


  private EventRecordQuery eventJournalQuery(final String entityId, final String tenant, EntityState<T> state) {
    return new EventRecordQuery(
      List.of(entityId),
      null,
      state.snapshot() != null ? state.snapshot().eventVersion() : null,
      null,
      null,
      null,
      null,
      new QueryOptions(
        "event_version",
        false,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        tenant
      )
    );
  }

  private EventRecordQuery ensureConsistencyQuery(final String entityId, final String tenant, EntityState<T> state) {
    // todo add idFrom to improve performance
    return new EventRecordQuery(
      List.of(entityId),
      null,
      state.currentEventVersion() != null ? state.currentEventVersion() : 0,
      null,
      null,
      null,
      null,
      new QueryOptions(
        "event_version",
        false,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        tenant
      )
    );
  }

  private Uni<EntityState<T>> processCommand(final EntityState<T> state, final List<Command> commands) {
    deDuplicate(state, commands);
    state.setProvisoryEventVersion(state.currentEventVersion());
    final var eventCommandTuple = commands.stream()
      .map(finalCommand -> applyBehaviour(state, finalCommand))
      .toList();
    return flattenEventsAndAggregate(state, eventCommandTuple);
  }

  private static void deDuplicate(EntityState<?> state, List<Command> commands) {
    if (state.commands() != null && !state.commands().isEmpty()) {
      state.commands().stream().filter(txId -> commands.stream().anyMatch(cmd -> cmd.headers().commandID().equals(txId)))
        .findAny()
        .ifPresent(duplicatedCommand -> {
            throw new CommandRejected(new EventXError("Command was already processed", "Command was carrying the same txId form a command previously processed -> " + duplicatedCommand, 400));
          }
        );
    }
  }

  private Uni<EntityState<T>> flattenEventsAndAggregate(final EntityState<T> state, final List<Tuple2<List<EventRecord>, Command>> eventCommandTuple) {
    final var flattenedEvents = eventCommandTuple.stream().map(Tuple2::getItem1).flatMap(List::stream).toList();
    if (configuration.persistenceMode() == PersistenceMode.DATABASE) {
      return aggregateEventsAndFlushToDisk(state, eventCommandTuple, flattenedEvents);
    } else {
      return aggregateEvents(state, eventCommandTuple);
    }
  }

  private Uni<EntityState<T>> aggregateEvents(final EntityState<T> state, final List<Tuple2<List<EventRecord>, Command>> eventCommandTuple) {
    LOGGER.warn("EntityAggregate will not be persisted to the database since persistence mode is set to -> " + configuration.persistenceMode());
    eventCommandTuple.forEach(tuple -> applyEvents(state, tuple.getItem1(), tuple.getItem2()));
    return cache.put(new AggregateKey(state.aggregateState().entityId(), state.aggregateState().tenantID()), state);
  }

  private Uni<EntityState<T>> aggregateEventsAndFlushToDisk(final EntityState<T> state, final List<Tuple2<List<EventRecord>, Command>> eventCommandTuple, final List<EventRecord> flattenedEvents) {
    eventCommandTuple.forEach(tuple -> applyEvents(state, tuple.getItem1(), tuple.getItem2()));
    return eventJournal.insertBatch(flattenedEvents)
      .flatMap(avoid -> cache.put(new AggregateKey(state.aggregateState().entityId(), state.aggregateState().tenantID()), state));
  }


  private Tuple2<List<EventRecord>, Command> applyBehaviour(final EntityState<T> state, final Command finalCommand) {
    final var currentVersion = state.provisoryEventVersion() == null ? 0 : state.provisoryEventVersion();
    final var events = applyBehaviour(state.aggregateState(), finalCommand);
    LOGGER.info("Events created " + new JsonArray(events).encodePrettily());
    final var array = events.toArray(new Object[0]);
    final var entityEvents = IntStream.range(1, array.length + 1)
      .mapToObj(index -> {
          final var ev = array[index - 1];
          final var nextEventVersion = currentVersion + index;
          state.setProvisoryEventVersion(nextEventVersion);
          return new EventRecord(
            finalCommand.entityId(),
            ev.getClass().getName(),
            nextEventVersion,
            JsonObject.mapFrom(ev),
            BaseRecord.newRecord(finalCommand.headers().tenantId())
          );
        }
      ).toList();
    return Tuple2.of(entityEvents, finalCommand);
  }


  private <C extends Command> Uni<EntityState<T>> processCommand(final EntityState<T> state, final C command) {
    if (state.commands() != null && !state.commands().isEmpty()) {
      state.commands().stream().filter(txId -> txId.equals(command.headers().commandID()))
        .findAny()
        .ifPresent(duplicatedCommand -> {
            throw new CommandRejected(new EventXError("Command was already processed", "Command was carrying the same txId form a command previously processed -> " + duplicatedCommand, 400));
          }
        );
    }
    state.setProvisoryEventVersion(state.currentEventVersion());
    final var eventCommandTuple = applyBehaviour(state, command);
    if (configuration.persistenceMode() == PersistenceMode.DATABASE) {
      return eventJournal.transaction(
        sqlConnection -> eventJournal.insertBatch(eventCommandTuple.getItem1(), sqlConnection)
          .flatMap(avoid2 -> {
              applyEvents(state, eventCommandTuple.getItem1(), command);
              return cache.put(new AggregateKey(command.entityId(), command.headers().tenantId()), state);
            }
          )
      );
    } else {
      LOGGER.warn("EntityAggregate will not be persisted to the database since persistence mode is set to -> " + configuration.persistenceMode());
      applyEvents(state, eventCommandTuple.getItem1(), command);
      return cache.put(new AggregateKey(command.entityId(), command.headers().tenantId()), state);
    }
  }

  private void handleSnapshot(EntityState<T> state) {
    if (snapshots != null && state.snapshotAfter() != null && state.processedEventsAfterLastSnapshot() >= state.snapshotAfter()) {
      if (Boolean.TRUE.equals(state.snapshotPresent())) {
        LOGGER.info("Updating snapshot -> " + state);
        final var snapshot = state.snapshot().withState(JsonObject.mapFrom(state));
        snapshots.updateByKey(snapshot)
          .flatMap(avoid -> updateStateAfterSnapshot(state, snapshot))
          .subscribe()
          .with(
            item -> LOGGER.info("Snapshot added -> " + item)
            , throwable -> LOGGER.error("Unable to add snapshot", throwable));
      } else {
        LOGGER.info("Adding snapshot -> " + state);
        final var snapshot = new AggregateSnapshot(state.aggregateState().entityId(), state.currentEventVersion(), JsonObject.mapFrom(state.aggregateState()), BaseRecord.newRecord(state.aggregateState().tenantID()));
        snapshots.insert(snapshot)
          .flatMap(avoid -> updateStateAfterSnapshot(state, snapshot))
          .subscribe()
          .with(
            item -> LOGGER.info("Snapshot added -> " + item),
            throwable -> LOGGER.error("Unable to add snapshot", throwable)
          );
      }
    }
  }

  private Uni<EntityState<T>> updateStateAfterSnapshot(final EntityState<T> state, final AggregateSnapshot snapshot) {
    state.eventsAfterSnapshot().clear();
    state.setProcessedEventsAfterLastSnapshot(0).setSnapshot(snapshot);
    return cache.put(new AggregateKey(state.aggregateState().entityId(), state.aggregateState().tenantID()), state);
  }

  private void handleRejectedCommand(final Throwable throwable, final Command command) {
    LOGGER.error("Command rejected -> ", throwable);
    if (configuration.persistenceMode() == PersistenceMode.DATABASE && rejectedCommands != null) {
      if (throwable instanceof CommandRejected commandRejected) {
        final var rejectedCommand = new RejectedCommand(command.entityId(), JsonObject.mapFrom(command), command.getClass().getName(), JsonObject.mapFrom(commandRejected.error()), BaseRecord.newRecord(command.headers().tenantId()));
        rejectedCommands.insertAndForget(rejectedCommand);
      } else {
        LOGGER.warn("Unknown exception, consider using RejectedCommandException.class for better error handling");
        final var cause = throwable.getCause() != null ? throwable.getCause().getMessage() : throwable.getLocalizedMessage();
        final var rejectedCommand = new RejectedCommand(command.entityId(), JsonObject.mapFrom(command), command.getClass().getName(), JsonObject.mapFrom(new EventXError(throwable.getMessage(), cause, 500)), BaseRecord.newRecord(command.headers().tenantId()));
        rejectedCommands.insertAndForget(rejectedCommand);
      }
    }
  }


  private Command getCommand(final String commandType, final JsonObject jsonCommand) {
    try {
      final var clazz = Class.forName(Objects.requireNonNullElse(commandClassMap.get(commandType), commandType));
      final var object = jsonCommand.mapTo(clazz);
      if (object instanceof Command command) {
        return command;
      } else {
        throw new CommandRejected(new EventXError("Command is not an instance of EntityAggregateCommand", JsonObject.mapFrom(object).encode(), 500));
      }
    } catch (Exception e) {
      LOGGER.error("Unable to cast command", e);
      throw new CommandRejected(new EventXError("Unable to cast class", e.getMessage(), 500));
    }
  }

  private Object getEvent(final String eventType, JsonObject event) {
    try {
      final var eventClass = Class.forName(eventType);
      return event.mapTo(eventClass);
    } catch (Exception e) {
      LOGGER.error("Unable to cast event", e);
      throw new CommandRejected(new EventXError("Unable to cast event", e.getMessage(), 500));
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
    str = str
      .replaceAll(
        regex, replacement)
      .toLowerCase();

    // return string
    return str;
  }

}
