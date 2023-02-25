package io.vertx.skeleton.evs.actors;

import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.skeleton.evs.*;
import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.evs.cache.EntityCache;
import io.vertx.skeleton.evs.exceptions.CommandRejected;
import io.vertx.skeleton.evs.exceptions.UnknownCommand;
import io.vertx.skeleton.evs.exceptions.EventException;
import io.vertx.skeleton.evs.objects.*;
import io.vertx.skeleton.sql.exceptions.OrmConflictException;
import io.vertx.skeleton.sql.exceptions.OrmNotFoundException;
import io.vertx.skeleton.sql.Repository;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.sql.models.BaseRecord;
import io.vertx.skeleton.sql.models.EmptyQuery;
import io.vertx.skeleton.sql.models.QueryOptions;

import java.util.*;
import java.util.stream.IntStream;

public class ActorLogic<T extends Entity> {
  private final List<BehaviourWrapper> commandBehaviours;
  // todo create an interface for the eventJournal
  // todo move this repository as an implementation of the eventJournal
  private final Repository<EntityEventKey, EntityEvent, EventJournalQuery> eventJournal;
  private final Repository<EntityKey, RejectedCommand, ?> rejectedCommandRepository;
  // todo create an interface for the snapshots
  // todo move this repository as an implementation of the snapshot
  private final Repository<EntityKey, AggregateSnapshot, ?> snapshotRepository;
  // todo create an interface for the entityCache
  // todo move this cache as an implementation of the entity cache
  private final EntityCache<T, EntityState<T>> cache;
  private static final Logger LOGGER = LoggerFactory.getLogger(ActorLogic.class);
  private final EntityConfiguration entityConfiguration;
  private final List<AggregatorWrapper> eventBehaviours;
  private final Class<T> entityAggregateClass;


  private final Map<String, String> commandClassMap = new HashMap<>();

  public ActorLogic(
    final Class<T> entityAggregateClass,
    final List<AggregatorWrapper> eventBehaviours,
    final List<BehaviourWrapper> commandBehaviours,
    final EntityConfiguration entityConfiguration,
    final Repository<EntityEventKey, EntityEvent, EventJournalQuery> eventJournal,
    final Repository<EntityKey, AggregateSnapshot, EmptyQuery> snapshotRepository,
    final Repository<EntityKey, RejectedCommand, ?> rejectedCommandRepository,
    final EntityCache<T, EntityState<T>> cache
  ) {
    this.eventJournal = eventJournal;
    this.entityAggregateClass = entityAggregateClass;
    this.eventBehaviours = eventBehaviours;
    this.commandBehaviours = commandBehaviours;
    this.snapshotRepository = snapshotRepository;
    this.rejectedCommandRepository = rejectedCommandRepository;
    this.entityConfiguration = entityConfiguration;
    this.cache = cache;
    populateCommandClassMap(commandBehaviours);
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
        commandClassMap.put(tuple.getItem2(), tuple.getItem1());
        commandClassMap.put(tuple.getItem3(), tuple.getItem1());
        commandClassMap.put(tuple.getItem4(), tuple.getItem1());
      }
    );
  }

  public Uni<JsonObject> load(EntityKey entityAggregateKey) {
    LOGGER.debug("Loading entity locally  -> " + entityAggregateKey);
    return load(entityAggregateKey.entityId(), entityAggregateKey.tenant(), ConsistencyStrategy.STRONGLY_CONSISTENT)
      .map(state -> JsonObject.mapFrom(state.aggregateState()));
  }


  public Uni<JsonObject> process(CompositeCommandWrapper command) {
    LOGGER.debug("Processing CompositeCommand -> " + command);
    final var commands = command.commands().stream()
      .map(cmd -> getCommand(cmd.commandType(), cmd.command()))
      .toList();
    if (!commands.stream().allMatch(cmd -> cmd.entityId().equals(command.entityId()))) {
      throw new CommandRejected(new Error("ID mismatch in composite command", "All composite commands should have the same entityId", 400));
    }
    return load(command.entityId(), command.commandHeaders().tenantId(), ConsistencyStrategy.EVENTUALLY_CONSISTENT)
      .flatMap(
        aggregateState -> processCommand(aggregateState, commands)
          .onFailure(OrmConflictException.class).recoverWithUni(
            () -> ensureConsistency(aggregateState, command.entityId(), command.commandHeaders().tenantId())
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
    return load(command.entityId(), command.commandHeaders().tenantId(), ConsistencyStrategy.EVENTUALLY_CONSISTENT)
      .flatMap(
        aggregateState -> processCommand(aggregateState, command)
          .onFailure(OrmConflictException.class).recoverWithUni(
            () -> ensureConsistency(aggregateState, command.entityId(), command.commandHeaders().tenantId())
              .flatMap(reconstructedState -> processCommand(reconstructedState, command))
          )
          .onFailure().invoke(throwable -> handleRejectedCommand(throwable, command))
      )
      .invoke(this::handleSnapshot)
      .map(state -> JsonObject.mapFrom(state.aggregateState()));
  }

  private T applyEventBehaviour(T aggregateState, final Object event) {
    final var customBehaviour = eventBehaviours.stream()
      .filter(behaviour -> behaviour.delegate().tenantId() != null && behaviour.delegate().tenantId().equals(aggregateState.tenantID()))
      .filter(behaviour -> behaviour.eventClass().isAssignableFrom(event.getClass()))
      .findFirst()
      .orElse(null);
    if (customBehaviour == null) {
      final var defaultBehaviour = eventBehaviours.stream()
        .filter(behaviour -> behaviour.delegate().tenantId() == null)
        .filter(behaviour -> behaviour.eventClass().isAssignableFrom(event.getClass()))
        .findFirst()
        .orElseThrow(() -> EventException.unknown(event.getClass()));
      LOGGER.info("Applying behaviour -> " + defaultBehaviour.getClass().getSimpleName());
      return (T) defaultBehaviour.delegate().apply(aggregateState, event);
    }
    LOGGER.info("Applying behaviour -> " + customBehaviour.getClass().getSimpleName());
    return (T) customBehaviour.delegate().apply(aggregateState, event);
  }

  private List<Object> applyCommandBehaviour(final T aggregateState, final Command command) {
    final var customBehaviour = commandBehaviours.stream()
      .filter(behaviour -> behaviour.delegate().tenantID() != null && behaviour.delegate().tenantID().equals(command.commandHeaders().tenantId()))
      .filter(behaviour -> behaviour.commandClass().isAssignableFrom(command.getClass()))
      .findFirst()
      .orElse(null);
    if (customBehaviour == null) {
      final var defaultBehaviour = commandBehaviours.stream()
        .filter(behaviour -> behaviour.delegate().tenantID() == null)
        .filter(behaviour -> behaviour.commandClass().isAssignableFrom(command.getClass()))
        .findFirst()
        .orElseThrow(() -> UnknownCommand.unknown(command.getClass()));
      LOGGER.info("Applying command Behaviour -> " + defaultBehaviour.getClass().getSimpleName());
      return defaultBehaviour.delegate().process(aggregateState, command);
    }
    LOGGER.info("Applying command behaviour -> " + customBehaviour.getClass().getSimpleName());
    return customBehaviour.delegate().process(aggregateState, command);
  }

  private Uni<EntityState<T>> load(String entityId, String tenant, ConsistencyStrategy consistencyStrategy) {
    LOGGER.debug("Loading entity aggregate " + entityId + " with consistency strategy -> " + consistencyStrategy);
    return switch (consistencyStrategy) {
      case EVENTUALLY_CONSISTENT -> {
        EntityState<T> state = null;
        if (cache != null) {
          state = cache.get(new EntityKey(entityId, tenant));
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
          state = cache.get(new EntityKey(entityId, tenant));
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
    if (entityConfiguration.persistenceMode() == PersistenceMode.DATABASE) {
      LOGGER.warn("Ensuring consistency for entity -> " + entityId);
      return eventJournal.query(ensureConsistencyQuery(entityId, tenant, state))
        .flatMap(events -> {
            if (events != null && !events.isEmpty()) {
              applyEvents(state, events, null);
            }
            return cache.put(new EntityKey(entityId, tenant), state);
          }
        )
        .onFailure(OrmNotFoundException.class).recoverWithItem(state);
    } else {
      LOGGER.warn("Consistency not ensure since persistence mode is set to -> " + entityConfiguration.persistenceMode());
      return Uni.createFrom().item(state);
    }
  }

  private Uni<EntityState<T>> loadFromDatabase(final String entityId, final String tenant) {
    final var state = new EntityState<T>(
      entityConfiguration.snapshotEvery(),
      entityConfiguration.maxNumberOfCommandsForIdempotency()
    );
    if (entityConfiguration.persistenceMode() == PersistenceMode.DATABASE) {
      LOGGER.warn("Loading entity from database -> " + entityId);
      Uni<Void> snapshotUni = Uni.createFrom().voidItem();
      if (snapshotRepository != null) {
        snapshotUni = snapshotRepository.selectByKey(new EntityKey(entityId, tenant))
          .map(persistedSnapshot -> loadSnapshot(state, persistedSnapshot))
          .onFailure(OrmNotFoundException.class).recoverWithNull()
          .replaceWithVoid();
      }
      return snapshotUni.flatMap(avoid -> eventJournal.query(eventJournalQuery(entityId, tenant, state)))
        .flatMap(events -> {
            if (events != null && !events.isEmpty()) {
              applyEvents(state, events, null);
            }
            return cache.put(new EntityKey(entityId, tenant), state);
          }
        )
        .onFailure(OrmNotFoundException.class).recoverWithItem(state);
    } else {
      LOGGER.warn("Wont look for entity in database since persistence mode is set to -> " + entityConfiguration.persistenceMode());
      return Uni.createFrom().item(state);
    }

  }

  private EntityState<T> loadSnapshot(final EntityState<T> state, final AggregateSnapshot persistedSnapshot) {
    LOGGER.info("Loading snapshot -> " + persistedSnapshot.state().encodePrettily());
    return state.setAggregateState(persistedSnapshot.state().mapTo(entityAggregateClass))
      .setCurrentEventVersion(persistedSnapshot.eventVersion())
      .setSnapshot(persistedSnapshot)
      .setSnapshotPresent(true);
  }

  private void applyEvents(final EntityState<T> state, final List<EntityEvent> events, Command command) {
    events.stream()
      .sorted(Comparator.comparingLong(EntityEvent::eventVersion))
      .forEachOrdered(event -> {
          LOGGER.info("Aggregating event -> " + event.eventClass());
          final var newState = applyEventBehaviour(state.aggregateState(), getEvent(event.eventClass(), event.event()));
          LOGGER.info("New aggregate state -> " + newState);
          if (command != null && state.commands() != null && state.commands().stream().noneMatch(txId -> txId.equals(command.commandHeaders().requestID()))) {
            state.commands().add(command.commandHeaders().requestID());
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


  private EventJournalQuery eventJournalQuery(final String entityId, final String tenant, EntityState<T> state) {
    return new EventJournalQuery(
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

  private EventJournalQuery ensureConsistencyQuery(final String entityId, final String tenant, EntityState<T> state) {
    // todo add idFrom to improve performance
    return new EventJournalQuery(
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
    if (state.commands() != null && !state.commands().isEmpty()) {
      state.commands().stream().filter(txId -> commands.stream().anyMatch(cmd -> cmd.commandHeaders().requestID().equals(txId)))
        .findAny()
        .ifPresent(duplicatedCommand -> {
            throw new CommandRejected(new Error("Command was already processed", "Command was carrying the same txId form a command previously processed -> " + duplicatedCommand, 400));
          }
        );
    }
    state.setProvisoryEventVersion(state.currentEventVersion());
    final var eventCommandTuple = commands.stream()
      .map(finalCommand -> processSingleCommand(state, finalCommand))
      .toList();
    return handleEvents(state, eventCommandTuple);
  }

  private Uni<EntityState<T>> handleEvents(final EntityState<T> state, final List<Tuple2<List<EntityEvent>, Command>> eventCommandTuple) {
    final var flattenedEvents = eventCommandTuple.stream().map(Tuple2::getItem1).flatMap(List::stream).toList();
    if (entityConfiguration.persistenceMode() == PersistenceMode.DATABASE) {
      return persistEventsUpdateStateAndProjections(state, eventCommandTuple, flattenedEvents);
    } else {
      return updateStateAndProjections(state, eventCommandTuple);
    }
  }

  private Uni<EntityState<T>> updateStateAndProjections(final EntityState<T> state, final List<Tuple2<List<EntityEvent>, Command>> eventCommandTuple) {
    LOGGER.warn("EntityAggregate will not be persisted to the database since persistence mode is set to -> " + entityConfiguration.persistenceMode());
    eventCommandTuple.forEach(tuple -> applyEvents(state, tuple.getItem1(), tuple.getItem2()));
    return cache.put(new EntityKey(state.aggregateState().entityId(), state.aggregateState().tenantID()), state);
  }

  private Uni<EntityState<T>> persistEventsUpdateStateAndProjections(final EntityState<T> state, final List<Tuple2<List<EntityEvent>, Command>> eventCommandTuple, final List<EntityEvent> flattenedEvents) {
    return eventJournal.transaction(
      sqlConnection -> eventJournal.insertBatch(flattenedEvents, sqlConnection)
        .flatMap(avoid2 -> {
            eventCommandTuple.forEach(tuple -> applyEvents(state, tuple.getItem1(), tuple.getItem2()));
            return cache.put(new EntityKey(state.aggregateState().entityId(), state.aggregateState().tenantID()), state);
          }
        )
    );
  }

  private Tuple2<List<EntityEvent>, Command> processSingleCommand(final EntityState<T> state, final Command finalCommand) {
    final var currentVersion = state.provisoryEventVersion() == null ? 0 : state.provisoryEventVersion();
    final var events = applyCommandBehaviour(state.aggregateState(), finalCommand);
    final var array = events.toArray(new Object[0]);
    final var entityEvents = IntStream.range(1, array.length + 1)
      .mapToObj(index -> {
          final var ev = array[index - 1];
          final var nextEventVersion = currentVersion + index;
          state.setProvisoryEventVersion(nextEventVersion);
          return new EntityEvent(
            finalCommand.entityId(),
            ev.getClass().getName(),
            nextEventVersion,
            JsonObject.mapFrom(ev),
            JsonObject.mapFrom(finalCommand),
            finalCommand.getClass().getName(),
            BaseRecord.newRecord(finalCommand.commandHeaders().tenantId())
          );
        }
      ).toList();
    return Tuple2.of(entityEvents, finalCommand);
  }


  private <C extends Command> Uni<EntityState<T>> processCommand(final EntityState<T> state, final C command) {
    if (state.commands() != null && !state.commands().isEmpty()) {
      state.commands().stream().filter(txId -> txId.equals(command.commandHeaders().requestID()))
        .findAny()
        .ifPresent(duplicatedCommand -> {
            throw new CommandRejected(new Error("Command was already processed", "Command was carrying the same txId form a command previously processed -> " + duplicatedCommand, 400));
          }
        );
    }
    state.setProvisoryEventVersion(state.currentEventVersion());
    final var eventCommandTuple = processSingleCommand(state, command);
    if (entityConfiguration.persistenceMode() == PersistenceMode.DATABASE) {
      return eventJournal.transaction(
        sqlConnection -> eventJournal.insertBatch(eventCommandTuple.getItem1(), sqlConnection)
          .flatMap(avoid2 -> {
              applyEvents(state, eventCommandTuple.getItem1(), command);
              return cache.put(new EntityKey(command.entityId(), command.commandHeaders().tenantId()), state);
            }
          )
      );
    } else {
      LOGGER.warn("EntityAggregate will not be persisted to the database since persistence mode is set to -> " + entityConfiguration.persistenceMode());
      applyEvents(state, eventCommandTuple.getItem1(), command);
      return cache.put(new EntityKey(command.entityId(), command.commandHeaders().tenantId()), state);
    }
  }

  private void handleSnapshot(EntityState<T> state) {
    if (snapshotRepository != null && state.snapshotAfter() != null && state.processedEventsAfterLastSnapshot() >= state.snapshotAfter()) {
      if (Boolean.TRUE.equals(state.snapshotPresent())) {
        LOGGER.info("Updating snapshot -> " + state);
        final var snapshot = state.snapshot().withState(JsonObject.mapFrom(state));
        snapshotRepository.updateByKey(snapshot)
          .flatMap(avoid -> updateStateAfterSnapshot(state, snapshot))
          .subscribe()
          .with(
            item -> LOGGER.info("Snapshot added -> " + item)
            , throwable -> LOGGER.error("Unable to add snapshot", throwable));
      } else {
        LOGGER.info("Adding snapshot -> " + state);
        final var snapshot = new AggregateSnapshot(state.aggregateState().entityId(), state.currentEventVersion(), JsonObject.mapFrom(state.aggregateState()), BaseRecord.newRecord(state.aggregateState().tenantID()));
        snapshotRepository.insert(snapshot)
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
    return cache.put(new EntityKey(state.aggregateState().entityId(), state.aggregateState().tenantID()), state);
  }

  private void handleRejectedCommand(final Throwable throwable, final Command command) {
    LOGGER.error("Command rejected -> ", throwable);
    if (entityConfiguration.persistenceMode() == PersistenceMode.DATABASE && rejectedCommandRepository != null) {
      if (throwable instanceof CommandRejected commandRejected) {
        final var rejectedCommand = new RejectedCommand(command.entityId(), JsonObject.mapFrom(command), command.getClass().getName(), JsonObject.mapFrom(commandRejected.error()), BaseRecord.newRecord(command.commandHeaders().tenantId()));
        rejectedCommandRepository.insertAndForget(rejectedCommand);
      } else {
        LOGGER.warn("Unknown exception, consider using RejectedCommandException.class for better error handling");
        final var cause = throwable.getCause() != null ? throwable.getCause().getMessage() : throwable.getLocalizedMessage();
        final var rejectedCommand = new RejectedCommand(command.entityId(), JsonObject.mapFrom(command), command.getClass().getName(), JsonObject.mapFrom(new Error(throwable.getMessage(), cause, 500)), BaseRecord.newRecord(command.commandHeaders().tenantId()));
        rejectedCommandRepository.insertAndForget(rejectedCommand);
      }
    }
  }


  private Command getCommand(final String commandType, final JsonObject jsonCommand) {
    try {
      final var clazz = Class.forName(commandClassMap.get(commandType));
      final var object = jsonCommand.mapTo(clazz);
      if (object instanceof Command command) {
        return command;
      } else {
        throw new CommandRejected(new Error("Command is not an instance of EntityAggregateCommand", JsonObject.mapFrom(object).encode(), 500));
      }
    } catch (Exception e) {
      LOGGER.error("Unable to cast command", e);
      throw new CommandRejected(new Error("Unable to cast class", e.getMessage(), 500));
    }
  }

  private Object getEvent(final String eventType, JsonObject event) {
    try {
      final var eventClass = Class.forName(eventType);
      return event.mapTo(eventClass);
    } catch (Exception e) {
      LOGGER.error("Unable to cast event", e);
      throw new CommandRejected(new Error("Unable to cast event", e.getMessage(), 500));
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
