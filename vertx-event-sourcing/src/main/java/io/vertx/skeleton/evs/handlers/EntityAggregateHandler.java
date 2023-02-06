package io.vertx.skeleton.evs.handlers;

import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.skeleton.evs.*;
import io.vertx.skeleton.evs.cache.EntityAggregateCache;
import io.vertx.skeleton.evs.exceptions.UnknownCommandException;
import io.vertx.skeleton.evs.exceptions.UnknownEventException;
import io.vertx.skeleton.evs.exceptions.UnknownQueryException;
import io.vertx.skeleton.evs.objects.*;
import io.vertx.skeleton.models.*;
import io.vertx.skeleton.orm.Repository;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.vertx.UniHelper;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.skeleton.models.Error;

import java.util.*;
import java.util.stream.IntStream;

import static io.activej.inject.binding.Multibinders.toMap;

public class EntityAggregateHandler<T extends EntityAggregate> {
  private final List<CommandBehaviourWrapper> commandBehaviours;
  private final Repository<EntityEventKey, EntityEvent, EventJournalQuery> eventJournal;
  private final Repository<EntityAggregateKey, RejectedCommand, ?> rejectedCommandRepository;
  private final Repository<EntityAggregateKey, AggregateSnapshot, ?> snapshotRepository;
  private final EntityAggregateCache<T, EntityAggregateState<T>> cache;
  private static final Logger LOGGER = LoggerFactory.getLogger(EntityAggregateHandler.class);
  private final List<ProjectionWrapper> projections;
  private final EntityAggregateConfiguration entityAggregateConfiguration;
  private final PersistenceMode persistenceMode;
  private final List<EventBehaviourWrapper> eventBehaviours;
  private final Class<T> entityAggregateClass;
  private final List<QueryBehaviourWrapper> queryBehaviours;

  List<CommandValidatorWrapper> commandValidatorV2s;

  private final Map<String, String> commandClassMap = new HashMap<>();

  public EntityAggregateHandler(
    final Class<T> entityAggregateClass,
    final List<CommandValidatorWrapper> commandValidatorV2s,
    final List<EventBehaviourWrapper> eventBehaviours,
    final List<CommandBehaviourWrapper> commandBehaviours,
    final List<QueryBehaviourWrapper> queryBehaviours,
    final List<ProjectionWrapper> projections,
    final EntityAggregateConfiguration entityAggregateConfiguration,
    final Repository<EntityEventKey, EntityEvent, EventJournalQuery> eventJournal,
    final Repository<EntityAggregateKey, AggregateSnapshot, EmptyQuery> snapshotRepository,
    final Repository<EntityAggregateKey, RejectedCommand, ?> rejectedCommandRepository,
    final EntityAggregateCache<T, EntityAggregateState<T>> cache,
    final PersistenceMode persistenceMode
  ) {
    this.eventJournal = eventJournal;
    this.entityAggregateClass = entityAggregateClass;
    this.commandValidatorV2s = commandValidatorV2s;
    this.eventBehaviours = eventBehaviours;
    this.commandBehaviours = commandBehaviours;
    this.projections = projections;
    this.queryBehaviours = queryBehaviours;
    this.snapshotRepository = snapshotRepository;
    this.rejectedCommandRepository = rejectedCommandRepository;
    this.entityAggregateConfiguration = entityAggregateConfiguration;
    this.cache = cache;
    this.persistenceMode = persistenceMode;
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

  public Uni<JsonObject> query(String className, JsonObject jsonQuery) {
    LOGGER.debug("Processing " + className + " -> " + jsonQuery.encodePrettily());
    final var query = getQuery(className, jsonQuery);
    final var behaviour = getQueryBehaviour(query.requestMetadata().tenant(), query);
    return load(query.entityId(), query.requestMetadata().tenant(), behaviour.strategy())
      .map(aggregateState -> behaviour.query(aggregateState.aggregateState(), query))
      .map(JsonObject::mapFrom);
  }

  private QueryBehaviour<T, EntityAggregateQuery> getQueryBehaviour(Tenant tenant, final EntityAggregateQuery query) {
    final var queryBehaviour = queryBehaviours.stream()
      .filter(behaviour -> behaviour.delegate().tenant().equals(tenant))
      .filter(behaviour -> behaviour.queryClass().getName().equals(query.getClass().getName()))
      .findFirst()
      .orElseThrow(() -> UnknownQueryException.unknown(query.getClass()));
    LOGGER.info("Applying custom query behaviour -> " + queryBehaviour.getClass().getSimpleName());
    return queryBehaviour.delegate();
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

  private List<Object> applyCommandBehaviour(final T aggregateState, final EntityAggregateCommand command) {
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
      LOGGER.info("Applying behaviour -> " + defaultBehaviour.getClass().getSimpleName());
      return defaultBehaviour.delegate().process(aggregateState, command);
    }
    LOGGER.info("Applying behaviour -> " + customBehaviour.getClass().getSimpleName());
    return customBehaviour.delegate().process(aggregateState, command);
  }

  private Uni<EntityAggregateCommand> applyCommandValidator(final EntityAggregateCommand command, T aggregateState) {
    final var customBehaviour = commandValidatorV2s.stream()
      .filter(behaviour -> behaviour.delegate().tenant() != null)
      .filter(behaviour -> behaviour.delegate().tenant().equals(command.requestMetadata().tenant()))
      .filter(behaviour -> behaviour.commandClass().getName().equals(command.getClass().getName()))
      .findFirst()
      .orElse(null);
    if (customBehaviour == null) {
      final var defaultValidator = commandValidatorV2s.stream()
        .filter(behaviour -> behaviour.delegate().tenant() == null)
        .filter(behaviour -> behaviour.commandClass().getName().equals(command.getClass().getName()))
        .findFirst()
        .orElse(null);
      if (defaultValidator != null) {
        LOGGER.info("Applying command validator -> " + defaultValidator.getClass().getSimpleName());
        return defaultValidator.delegate().validate(command, aggregateState);
      }
    }
    if (customBehaviour != null) {
      LOGGER.info("Applying command validator -> " + customBehaviour.getClass().getSimpleName());
      return customBehaviour.delegate().validate(command, aggregateState);
    }
    LOGGER.info("No command validators registered for command -> " + command.getClass().getSimpleName());
    return Uni.createFrom().item(command);
  }


//  private Projection<T> getProjection(final EntityAggregateQuery query) {
//    final var queryBehaviour = projections.stream()
//      .filter(behaviour -> behaviour.delegate().tenant().equals(query.requestMetadata().tenant()))
//      .filter(behaviour -> behaviour.delegate().projectionQueryClasses().stream().anyMatch(c -> c.getClass().isAssignableFrom(query.getClass())))
//      .findFirst()
//      .orElseThrow(() -> UnknownQueryException.unknown(query.getClass()));
//    LOGGER.info("Applying custom query behaviour -> " + queryBehaviour.getClass().getSimpleName());
//    return queryBehaviour.delegate();
//  }
//
//  public Uni<JsonObject> queryProjection(final String className, final JsonObject jsonQuery) {
//    LOGGER.debug("Processing " + className + " -> " + jsonQuery.encodePrettily());
//    final var query = getQuery(className, jsonQuery);
//    final var projection = getProjection(query);
//    return projection.queryProjection(query).map(JsonObject::mapFrom);
//  }

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
              consumeEvents(state, events, null);
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
              consumeEvents(state, events, null);
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

  private void consumeEvents(final EntityAggregateState<T> state, final List<EntityEvent> events, EntityAggregateCommand command) {
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

  private void updateEventuallyConsistentProjections(final T aggregateState, final List<EntityEvent> events) {
    eventJournal.transaction(sqlConnection -> updateProjections(aggregateState, events, ConsistencyStrategy.EVENTUALLY_CONSISTENT, sqlConnection))
      .subscribe()
      .with(UniHelper.NOOP, throwable -> LOGGER.error("Unable to update projections -> ", throwable));
  }

  private Uni<Void> updateProjections(final T aggregateState, final List<EntityEvent> events, ConsistencyStrategy consistencyStrategy, final SqlConnection sqlConnection) {
    final var uniList = new ArrayList<Uni<Void>>();
    if (projections != null && !projections.isEmpty() && projections.stream().anyMatch(p -> p.delegate().strategy() == consistencyStrategy)) {
      uniList.add(performUpdate(aggregateState, projections.stream().map(p -> (Projection<T>) p.delegate()).toList(), events, sqlConnection));
    }
    if (!uniList.isEmpty()) {
      return Uni.join().all(uniList).andFailFast().replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> performUpdate(final T aggregateState, final List<Projection<T>> projectionsToUpdate, final List<EntityEvent> events, final SqlConnection sqlConnection) {
    return Multi.createFrom().iterable(projectionsToUpdate)
      .onItem().transformToUniAndMerge(projection -> {
          final var matchingEvents = events.stream()
            .filter(event -> projection.eventsClasses() == null || projection.eventsClasses().stream().anyMatch(eventClass -> eventClass.getName().equals(event.eventClass())))
            .toList();
          if (!matchingEvents.isEmpty()) {
            LOGGER.info(projection.getClass().getSimpleName() + " interested in one or more events emitted from -> " + matchingEvents);
            final var parsedEvents = matchingEvents.stream().map(ev -> getEvent(ev.eventClass(), ev.event())).toList();
            return projection.applyEvents(aggregateState, parsedEvents, sqlConnection);
          }
          LOGGER.debug(projection.getClass().getSimpleName() + " is not interested in events");
          return Uni.createFrom().voidItem();
        }
      ).collect().asList()
      .replaceWithVoid();
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

  private Uni<EntityAggregateState<T>> processCommand(final EntityAggregateState<T> state, final List<EntityAggregateCommand> commands) {
    if (state.commands() != null && !state.commands().isEmpty()) {
      state.commands().stream().filter(txId -> commands.stream().anyMatch(cmd -> cmd.requestMetadata().txId().equals(txId)))
        .findAny()
        .ifPresent(duplicatedCommand -> {
            throw new RejectedCommandException(new Error("Command was already processed", "Command was carrying the same txId form a command previously processed -> " + duplicatedCommand, 400));
          }
        );
    }

    return Multi.createFrom().iterable(commands)
      .onItem().transformToUniAndMerge(cmd -> applyCommandValidator(cmd, state.aggregateState()))
      .collect().asList()
      .flatMap(finalCommands -> {
          state.setProvisoryEventVersion(state.currentEventVersion());
          final var eventCommandTuple = finalCommands.stream()
            .map(finalCommand -> processValidatedCommand(state, finalCommand))
            .toList();
          return handleEvents(state, eventCommandTuple);
        }
      );
  }

  private Uni<EntityAggregateState<T>> handleEvents(final EntityAggregateState<T> state, final List<Tuple2<List<EntityEvent>, EntityAggregateCommand>> eventCommandTuple) {
    final var flattenedEvents = eventCommandTuple.stream().map(Tuple2::getItem1).flatMap(List::stream).toList();
    if (entityAggregateConfiguration.persistenceMode() == PersistenceMode.DATABASE) {
      return persistEventsUpdateStateAndProjections(state, eventCommandTuple, flattenedEvents);
    } else {
      return updateStateAndProjections(state, eventCommandTuple);
    }
  }

  private Uni<EntityAggregateState<T>> updateStateAndProjections(final EntityAggregateState<T> state, final List<Tuple2<List<EntityEvent>, EntityAggregateCommand>> eventCommandTuple) {
    LOGGER.warn("EntityAggregate will not be persisted to the database since persistence mode is set to -> " + entityAggregateConfiguration.persistenceMode());
    eventCommandTuple.forEach(tuple -> consumeEvents(state, tuple.getItem1(), tuple.getItem2()));
    return cache.put(new EntityAggregateKey(state.aggregateState().entityId(), state.aggregateState().tenant()), state);
  }

  private Uni<EntityAggregateState<T>> persistEventsUpdateStateAndProjections(final EntityAggregateState<T> state, final List<Tuple2<List<EntityEvent>, EntityAggregateCommand>> eventCommandTuple, final List<EntityEvent> flattenedEvents) {
    return eventJournal.transaction(
        sqlConnection -> eventJournal.insertBatch(flattenedEvents, sqlConnection)
          .flatMap(avoid2 -> {
              eventCommandTuple.forEach(tuple -> consumeEvents(state, tuple.getItem1(), tuple.getItem2()));
              return updateProjections(state.aggregateState(), flattenedEvents, ConsistencyStrategy.STRONGLY_CONSISTENT, sqlConnection)
                .flatMap(avoid3 -> cache.put(new EntityAggregateKey(state.aggregateState().entityId(), state.aggregateState().tenant()), state));
            }
          )
      )
      .invoke(aggregateState -> updateEventuallyConsistentProjections(state.aggregateState(), flattenedEvents));
  }

  private <C extends EntityAggregateCommand> Tuple2<List<EntityEvent>, EntityAggregateCommand> processValidatedCommand(final EntityAggregateState<T> state, final EntityAggregateCommand finalCommand) {
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


  private <C extends EntityAggregateCommand> Uni<EntityAggregateState<T>> processCommand(final EntityAggregateState<T> state, final C command) {
    if (state.commands() != null && !state.commands().isEmpty()) {
      state.commands().stream().filter(txId -> txId.equals(command.requestMetadata().txId()))
        .findAny()
        .ifPresent(duplicatedCommand -> {
            throw new RejectedCommandException(new Error("Command was already processed", "Command was carrying the same txId form a command previously processed -> " + duplicatedCommand, 400));
          }
        );
    }
    return applyCommandValidator(command, state.aggregateState())
      .flatMap(finalCommand -> {
          state.setProvisoryEventVersion(state.currentEventVersion());
          final var eventCommandTuple = processValidatedCommand(state, finalCommand);
          if (entityAggregateConfiguration.persistenceMode() == PersistenceMode.DATABASE) {
            return eventJournal.transaction(
                sqlConnection -> eventJournal.insertBatch(eventCommandTuple.getItem1(), sqlConnection)
                  .flatMap(avoid2 -> {
                      consumeEvents(state, eventCommandTuple.getItem1(), finalCommand);
                      return updateProjections(state.aggregateState(), eventCommandTuple.getItem1(), ConsistencyStrategy.STRONGLY_CONSISTENT, sqlConnection)
                        .flatMap(avoid3 -> cache.put(new EntityAggregateKey(finalCommand.entityId(), finalCommand.requestMetadata().tenant()), state));
                    }
                  )
              )
              .invoke(aggregateState -> updateEventuallyConsistentProjections(state.aggregateState(), eventCommandTuple.getItem1()));
          } else {
            LOGGER.warn("EntityAggregate will not be persisted to the database since persistence mode is set to -> " + entityAggregateConfiguration.persistenceMode());
            consumeEvents(state, eventCommandTuple.getItem1(), finalCommand);
            return cache.put(new EntityAggregateKey(finalCommand.entityId(), finalCommand.requestMetadata().tenant()), state);
          }
        }
      );
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

  private void handleRejectedCommand(final Throwable throwable, final EntityAggregateCommand command) {
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


  private EntityAggregateCommand getCommand(final String commandType, final JsonObject jsonCommand) {
    try {
      final var clazz = Class.forName(commandClassMap.get(commandType));
      final var object = jsonCommand.mapTo(clazz);
      if (object instanceof EntityAggregateCommand entityAggregateCommand) {
        return entityAggregateCommand;
      } else {
        throw new RejectedCommandException(new Error("Command is not an instance of EntityAggregateCommand", JsonObject.mapFrom(object).encode(), 500));
      }
    } catch (Exception e) {
      LOGGER.error("Unable to cast command", e);
      throw new RejectedCommandException(new Error("Unable to cast class", e.getMessage(), 500));
    }
  }

  private static EntityAggregateQuery getQuery(final String type, final JsonObject jsonCommand) {
    try {
      final var clazz = Class.forName(type);
      final var object = jsonCommand.mapTo(clazz);
      if (object instanceof EntityAggregateQuery entityAggregateQuery) {
        return entityAggregateQuery;
      } else {
        throw new RejectedCommandException(new Error("Query is not an instance of EntityAggregateQuery", JsonObject.mapFrom(object).encode(), 500));
      }
    } catch (Exception e) {
      LOGGER.error("Unable to cast query", e);
      throw new RejectedCommandException(new Error("Unable to cast query", e.getMessage(), 500));
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
