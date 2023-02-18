package io.vertx.skeleton.evs.handlers;

import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.skeleton.evs.*;
import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.evs.cache.EntityAggregateCache;
import io.vertx.skeleton.evs.exceptions.RejectedCommandException;
import io.vertx.skeleton.evs.exceptions.UnknownCommandException;
import io.vertx.skeleton.evs.exceptions.UnknownEventException;
import io.vertx.skeleton.evs.objects.*;
import io.vertx.skeleton.models.*;
import io.vertx.skeleton.models.exceptions.OrmConflictException;
import io.vertx.skeleton.models.exceptions.OrmNotFoundException;
import io.vertx.skeleton.orm.Repository;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.models.Error;

import java.util.*;
import java.util.stream.IntStream;

public class EntityAggregateHandler<T extends EntityAggregate> {
  private final List<CommandBehaviourWrapper> commandBehaviours;
  // todo create an interface for the eventJournal
  // todo move this repository as an implementation of the eventJournal
  private final Repository<EntityEventKey, EntityEvent, EventJournalQuery> eventJournal;
  private final Repository<EntityAggregateKey, RejectedCommand, ?> rejectedCommandRepository;
  // todo create an interface for the snapshots
  // todo move this repository as an implementation of the snapshot
  private final Repository<EntityAggregateKey, AggregateSnapshot, ?> snapshotRepository;
  // todo create an interface for the entityCache
  // todo move this cache as an implementation of the entity cache
  private final EntityAggregateCache<T, EntityAggregateState<T>> cache;
  private static final Logger LOGGER = LoggerFactory.getLogger(EntityAggregateHandler.class);
  private final EntityAggregateConfiguration entityAggregateConfiguration;
  private final PersistenceMode persistenceMode;
  private final List<EventBehaviourWrapper> eventBehaviours;
  private final Class<T> entityAggregateClass;


  private final Map<String, String> commandClassMap = new HashMap<>();

  public EntityAggregateHandler(
    final Class<T> entityAggregateClass,
    final List<EventBehaviourWrapper> eventBehaviours,
    final List<CommandBehaviourWrapper> commandBehaviours,
    final EntityAggregateConfiguration entityAggregateConfiguration,
    final Repository<EntityEventKey, EntityEvent, EventJournalQuery> eventJournal,
    final Repository<EntityAggregateKey, AggregateSnapshot, EmptyQuery> snapshotRepository,
    final Repository<EntityAggregateKey, RejectedCommand, ?> rejectedCommandRepository,
    final EntityAggregateCache<T, EntityAggregateState<T>> cache,
    final PersistenceMode persistenceMode
  ) {
    this.eventJournal = eventJournal;
    this.entityAggregateClass = entityAggregateClass;
    this.eventBehaviours = eventBehaviours;
    this.commandBehaviours = commandBehaviours;
    this.snapshotRepository = snapshotRepository;
    this.rejectedCommandRepository = rejectedCommandRepository;
    this.entityAggregateConfiguration = entityAggregateConfiguration;
    this.cache = cache;
    this.persistenceMode = persistenceMode;
    populateCommandClassMap(commandBehaviours);
  }

  private void populateCommandClassMap(List<CommandBehaviourWrapper> commandBehaviours) {
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

  public Uni<JsonObject> load(EntityAggregateKey entityAggregateKey) {
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
      throw new RejectedCommandException(new Error("ID mismatch in composite command", "All composite commands should have the same entityId", 400));
    }
    return load(command.entityId(), command.requestMetadata().tenant(), ConsistencyStrategy.EVENTUALLY_CONSISTENT)
      .flatMap(
        aggregateState -> processCommand(aggregateState, commands)
          .onFailure(OrmConflictException.class).recoverWithUni(
            () -> ensureConsistency(aggregateState, command.entityId(), command.requestMetadata().tenant())
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
    return load(command.entityId(), command.requestMetadata().tenant(), ConsistencyStrategy.EVENTUALLY_CONSISTENT)
      .flatMap(
        aggregateState -> processCommand(aggregateState, command)
          .onFailure(OrmConflictException.class).recoverWithUni(
            () -> ensureConsistency(aggregateState, command.entityId(), command.requestMetadata().tenant())
              .flatMap(reconstructedState -> processCommand(reconstructedState, command))
          )
          .onFailure().invoke(throwable -> handleRejectedCommand(throwable, command))
      )
      .invoke(this::handleSnapshot)
      .map(state -> JsonObject.mapFrom(state.aggregateState()));
  }

  private T applyEventBehaviour(T aggregateState, final Object event) {
    final var customBehaviour = eventBehaviours.stream()
      .filter(behaviour -> behaviour.delegate().tenant() != null && behaviour.delegate().tenant().equals(aggregateState.tenant()))
      .filter(behaviour -> behaviour.eventClass().isAssignableFrom(event.getClass()))
      .findFirst()
      .orElse(null);
    if (customBehaviour == null) {
      final var defaultBehaviour = eventBehaviours.stream()
        .filter(behaviour -> behaviour.delegate().tenant() == null)
        .filter(behaviour -> behaviour.eventClass().isAssignableFrom(event.getClass()))
        .findFirst()
        .orElseThrow(() -> UnknownEventException.unknown(event.getClass()));
      LOGGER.info("Applying behaviour -> " + defaultBehaviour.getClass().getSimpleName());
      return (T) defaultBehaviour.delegate().apply(aggregateState, event);
    }
    LOGGER.info("Applying behaviour -> " + customBehaviour.getClass().getSimpleName());
    return (T) customBehaviour.delegate().apply(aggregateState, event);
  }

  private List<Object> applyCommandBehaviour(final T aggregateState, final Command command) {
    final var customBehaviour = commandBehaviours.stream()
      .filter(behaviour -> behaviour.delegate().tenant() != null && behaviour.delegate().tenant().equals(command.requestMetadata().tenant()))
      .filter(behaviour -> behaviour.commandClass().isAssignableFrom(command.getClass()))
      .findFirst()
      .orElse(null);
    if (customBehaviour == null) {
      final var defaultBehaviour = commandBehaviours.stream()
        .filter(behaviour -> behaviour.delegate().tenant() == null)
        .filter(behaviour -> behaviour.commandClass().isAssignableFrom(command.getClass()))
        .findFirst()
        .orElseThrow(() -> UnknownCommandException.unknown(command.getClass()));
      LOGGER.info("Applying command Behaviour -> " + defaultBehaviour.getClass().getSimpleName());
      return defaultBehaviour.delegate().process(aggregateState, command);
    }
    LOGGER.info("Applying command behaviour -> " + customBehaviour.getClass().getSimpleName());
    return customBehaviour.delegate().process(aggregateState, command);
  }

  private Uni<EntityAggregateState<T>> load(String entityId, Tenant tenant, ConsistencyStrategy consistencyStrategy) {
    LOGGER.debug("Loading entity aggregate " + entityId + " with consistency strategy -> " + consistencyStrategy);
    return switch (consistencyStrategy) {
      case EVENTUALLY_CONSISTENT -> {
        EntityAggregateState<T> state = null;
        if (cache != null) {
          state = cache.get(new EntityAggregateKey(entityId, tenant));
        }
        if (state == null) {
          yield loadFromDatabase(entityId, tenant);
        } else {
          yield Uni.createFrom().item(state);
        }
      }
      case STRONGLY_CONSISTENT -> {
        EntityAggregateState<T> state = null;
        if (cache != null) {
          state = cache.get(new EntityAggregateKey(entityId, tenant));
        }
        if (state == null) {
          yield loadFromDatabase(entityId, tenant);
        } else {
          yield ensureConsistency(state, entityId, tenant);
        }
      }
    };
  }

  private Uni<EntityAggregateState<T>> ensureConsistency(EntityAggregateState<T> state, final String entityId, final Tenant tenant) {
    if (persistenceMode == PersistenceMode.DATABASE) {
      LOGGER.warn("Ensuring consistency for entity -> " + entityId);
      return eventJournal.query(ensureConsistencyQuery(entityId, tenant, state))
        .flatMap(events -> {
            if (events != null && !events.isEmpty()) {
              applyEvents(state, events, null);
            }
            return cache.put(new EntityAggregateKey(entityId, tenant), state);
          }
        )
        .onFailure(OrmNotFoundException.class).recoverWithItem(state);
    } else {
      LOGGER.warn("Consistency not ensure since persistence mode is set to -> " + entityAggregateConfiguration.persistenceMode());
      return Uni.createFrom().item(state);
    }
  }

  private Uni<EntityAggregateState<T>> loadFromDatabase(final String entityId, final Tenant tenant) {
    final var state = new EntityAggregateState<T>(
      entityAggregateConfiguration.snapshotEvery(),
      entityAggregateConfiguration.maxNumberOfCommandsForIdempotency()
    );
    if (persistenceMode == PersistenceMode.DATABASE) {
      LOGGER.warn("Loading entity from database -> " + entityId);
      Uni<Void> snapshotUni = Uni.createFrom().voidItem();
      if (snapshotRepository != null) {
        snapshotUni = snapshotRepository.selectByKey(new EntityAggregateKey(entityId, tenant))
          .map(persistedSnapshot -> loadSnapshot(state, persistedSnapshot))
          .onFailure(OrmNotFoundException.class).recoverWithNull()
          .replaceWithVoid();
      }
      return snapshotUni.flatMap(avoid -> eventJournal.query(eventJournalQuery(entityId, tenant, state)))
        .flatMap(events -> {
            if (events != null && !events.isEmpty()) {
              applyEvents(state, events, null);
            }
            return cache.put(new EntityAggregateKey(entityId, tenant), state);
          }
        )
        .onFailure(OrmNotFoundException.class).recoverWithItem(state);
    } else {
      LOGGER.warn("Wont look for entity in database since persistence mode is set to -> " + entityAggregateConfiguration.persistenceMode());
      return Uni.createFrom().item(state);
    }

  }

  private EntityAggregateState<T> loadSnapshot(final EntityAggregateState<T> state, final AggregateSnapshot persistedSnapshot) {
    LOGGER.info("Loading snapshot -> " + persistedSnapshot.state().encodePrettily());
    return state.setAggregateState(persistedSnapshot.state().mapTo(entityAggregateClass))
      .setCurrentEventVersion(persistedSnapshot.eventVersion())
      .setSnapshot(persistedSnapshot)
      .setSnapshotPresent(true);
  }

  private void applyEvents(final EntityAggregateState<T> state, final List<EntityEvent> events, Command command) {
    events.stream()
      .sorted(Comparator.comparingLong(EntityEvent::eventVersion))
      .forEachOrdered(event -> {
          LOGGER.info("Aggregating event -> " + event.eventClass());
          final var newState = applyEventBehaviour(state.aggregateState(), getEvent(event.eventClass(), event.event()));
          LOGGER.info("New aggregate state -> " + newState);
          if (command != null && state.commands() != null && state.commands().stream().noneMatch(txId -> txId.equals(command.requestMetadata().txId()))) {
            state.commands().add(command.requestMetadata().txId());
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


  private EventJournalQuery eventJournalQuery(final String entityId, final Tenant tenant, EntityAggregateState<T> state) {
    return new EventJournalQuery(
      List.of(entityId),
      null,
      state.snapshot() != null ? state.snapshot().eventVersion() : null,
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

  private EventJournalQuery ensureConsistencyQuery(final String entityId, final Tenant tenant, EntityAggregateState<T> state) {
    return new EventJournalQuery(
      List.of(entityId),
      null,
      state.currentEventVersion() != null ? state.currentEventVersion() : 0,
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

  private Uni<EntityAggregateState<T>> processCommand(final EntityAggregateState<T> state, final List<Command> commands) {
    if (state.commands() != null && !state.commands().isEmpty()) {
      state.commands().stream().filter(txId -> commands.stream().anyMatch(cmd -> cmd.requestMetadata().txId().equals(txId)))
        .findAny()
        .ifPresent(duplicatedCommand -> {
            throw new RejectedCommandException(new Error("Command was already processed", "Command was carrying the same txId form a command previously processed -> " + duplicatedCommand, 400));
          }
        );
    }
    state.setProvisoryEventVersion(state.currentEventVersion());
    final var eventCommandTuple = commands.stream()
      .map(finalCommand -> processSingleCommand(state, finalCommand))
      .toList();
    return handleEvents(state, eventCommandTuple);
  }

  private Uni<EntityAggregateState<T>> handleEvents(final EntityAggregateState<T> state, final List<Tuple2<List<EntityEvent>, Command>> eventCommandTuple) {
    final var flattenedEvents = eventCommandTuple.stream().map(Tuple2::getItem1).flatMap(List::stream).toList();
    if (entityAggregateConfiguration.persistenceMode() == PersistenceMode.DATABASE) {
      return persistEventsUpdateStateAndProjections(state, eventCommandTuple, flattenedEvents);
    } else {
      return updateStateAndProjections(state, eventCommandTuple);
    }
  }

  private Uni<EntityAggregateState<T>> updateStateAndProjections(final EntityAggregateState<T> state, final List<Tuple2<List<EntityEvent>, Command>> eventCommandTuple) {
    LOGGER.warn("EntityAggregate will not be persisted to the database since persistence mode is set to -> " + entityAggregateConfiguration.persistenceMode());
    eventCommandTuple.forEach(tuple -> applyEvents(state, tuple.getItem1(), tuple.getItem2()));
    return cache.put(new EntityAggregateKey(state.aggregateState().entityId(), state.aggregateState().tenant()), state);
  }

  private Uni<EntityAggregateState<T>> persistEventsUpdateStateAndProjections(final EntityAggregateState<T> state, final List<Tuple2<List<EntityEvent>, Command>> eventCommandTuple, final List<EntityEvent> flattenedEvents) {
    return eventJournal.transaction(
      sqlConnection -> eventJournal.insertBatch(flattenedEvents, sqlConnection)
        .flatMap(avoid2 -> {
            eventCommandTuple.forEach(tuple -> applyEvents(state, tuple.getItem1(), tuple.getItem2()));
            return cache.put(new EntityAggregateKey(state.aggregateState().entityId(), state.aggregateState().tenant()), state);
          }
        )
    );
  }

  private Tuple2<List<EntityEvent>, Command> processSingleCommand(final EntityAggregateState<T> state, final Command finalCommand) {
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
            PersistedRecord.newRecord(finalCommand.requestMetadata().tenant())
          );
        }
      ).toList();
    return Tuple2.of(entityEvents, finalCommand);
  }


  private <C extends Command> Uni<EntityAggregateState<T>> processCommand(final EntityAggregateState<T> state, final C command) {
    if (state.commands() != null && !state.commands().isEmpty()) {
      state.commands().stream().filter(txId -> txId.equals(command.requestMetadata().txId()))
        .findAny()
        .ifPresent(duplicatedCommand -> {
            throw new RejectedCommandException(new Error("Command was already processed", "Command was carrying the same txId form a command previously processed -> " + duplicatedCommand, 400));
          }
        );
    }
    state.setProvisoryEventVersion(state.currentEventVersion());
    final var eventCommandTuple = processSingleCommand(state, command);
    if (entityAggregateConfiguration.persistenceMode() == PersistenceMode.DATABASE) {
      return eventJournal.transaction(
        sqlConnection -> eventJournal.insertBatch(eventCommandTuple.getItem1(), sqlConnection)
          .flatMap(avoid2 -> {
              applyEvents(state, eventCommandTuple.getItem1(), command);
              return cache.put(new EntityAggregateKey(command.entityId(), command.requestMetadata().tenant()), state);
            }
          )
      );
    } else {
      LOGGER.warn("EntityAggregate will not be persisted to the database since persistence mode is set to -> " + entityAggregateConfiguration.persistenceMode());
      applyEvents(state, eventCommandTuple.getItem1(), command);
      return cache.put(new EntityAggregateKey(command.entityId(), command.requestMetadata().tenant()), state);
    }
  }

  private void handleSnapshot(EntityAggregateState<T> state) {
    if (snapshotRepository != null && state.snapshotAfter() != null && state.processedEventsAfterLastSnapshot() >= state.snapshotAfter()) {
      if (Boolean.TRUE.equals(state.snapshotPresent())) {
        LOGGER.info("Updating snapshot -> " + state);
        final var snapshot = state.snapshot().withState(JsonObject.mapFrom(state));
        snapshotRepository.updateById(snapshot)
          .flatMap(avoid -> updateStateAfterSnapshot(state, snapshot))
          .subscribe()
          .with(
            item -> LOGGER.info("Snapshot added -> " + item)
            , throwable -> LOGGER.error("Unable to add snapshot", throwable));
      } else {
        LOGGER.info("Adding snapshot -> " + state);
        final var snapshot = new AggregateSnapshot(state.aggregateState().entityId(), state.currentEventVersion(), JsonObject.mapFrom(state.aggregateState()), PersistedRecord.newRecord(state.aggregateState().tenant()));
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

  private Uni<EntityAggregateState<T>> updateStateAfterSnapshot(final EntityAggregateState<T> state, final AggregateSnapshot snapshot) {
    state.eventsAfterSnapshot().clear();
    state.setProcessedEventsAfterLastSnapshot(0).setSnapshot(snapshot);
    return cache.put(new EntityAggregateKey(state.aggregateState().entityId(), state.aggregateState().tenant()), state);
  }

  private void handleRejectedCommand(final Throwable throwable, final Command command) {
    LOGGER.error("Command rejected -> ", throwable);
    if (entityAggregateConfiguration.persistenceMode() == PersistenceMode.DATABASE && rejectedCommandRepository != null) {
      if (throwable instanceof RejectedCommandException rejectedCommandException) {
        final var rejectedCommand = new RejectedCommand(command.entityId(), JsonObject.mapFrom(command), command.getClass().getName(), JsonObject.mapFrom(rejectedCommandException.error()), PersistedRecord.newRecord(command.requestMetadata().tenant()));
        rejectedCommandRepository.insertAndForget(rejectedCommand);
      } else {
        LOGGER.warn("Unknown exception, consider using RejectedCommandException.class for better error handling");
        final var cause = throwable.getCause() != null ? throwable.getCause().getMessage() : throwable.getLocalizedMessage();
        final var rejectedCommand = new RejectedCommand(command.entityId(), JsonObject.mapFrom(command), command.getClass().getName(), JsonObject.mapFrom(new Error(throwable.getMessage(), cause, 500)), PersistedRecord.newRecord(command.requestMetadata().tenant()));
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
        throw new RejectedCommandException(new Error("Command is not an instance of EntityAggregateCommand", JsonObject.mapFrom(object).encode(), 500));
      }
    } catch (Exception e) {
      LOGGER.error("Unable to cast command", e);
      throw new RejectedCommandException(new Error("Unable to cast class", e.getMessage(), 500));
    }
  }

  private Object getEvent(final String eventType, JsonObject event) {
    try {
      final var eventClass = Class.forName(eventType);
      return event.mapTo(eventClass);
    } catch (Exception e) {
      LOGGER.error("Unable to cast event", e);
      throw new RejectedCommandException(new Error("Unable to cast event", e.getMessage(), 500));
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
