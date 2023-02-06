package io.vertx.skeleton.evs.handlers;

import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.cache.EntityAggregateCache;
import io.vertx.skeleton.evs.objects.*;
import io.vertx.skeleton.models.*;
import io.vertx.skeleton.orm.Repository;
import io.vertx.skeleton.orm.RepositoryHandler;
import io.vertx.skeleton.orm.RepositoryMapper;
import io.vertx.skeleton.orm.mappers.AggregateSnapshotMapper;
import io.vertx.skeleton.orm.mappers.EventJournalMapper;
import io.vertx.skeleton.orm.mappers.RejectedCommandMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.models.Error;

import java.util.*;

public class EntityAggregateHandlerVerticle<T extends EntityAggregate> extends AbstractVerticle {
  protected static final Logger LOGGER = LoggerFactory.getLogger(EntityAggregateHandlerVerticle.class);
  public static final String ACTION = "action";
  public static final String CLASS_NAME = "className";
  private final RepositoryMapper<EntityEventKey, EntityEvent, EventJournalQuery> eventJournalMapper;
  private final RepositoryMapper<EntityAggregateKey, AggregateSnapshot, EmptyQuery> snapshotRepositoryMapper;
  private final RepositoryMapper<EntityAggregateKey, RejectedCommand, EmptyQuery> rejectCommandMapper;
  private final List<ProjectionWrapper> projections;
  private final List<CommandBehaviourWrapper> commandBehaviours;
  private final List<EventBehaviourWrapper> eventBehaviours;
  private final List<CommandValidatorWrapper> commandValidators;
  private final List<QueryBehaviourWrapper> queryBehaviours;
  private EntityAggregateCache<T, EntityAggregateState<T>> entityAggregateCache = null;
  private final Class<T> entityAggregateClass;
  private final EntityAggregateConfiguration entityAggregateConfiguration;
  private RepositoryHandler repositoryHandler;
  public EntityAggregateHandler<T> entityAggregateHandler;
  private final Map<String, String> commandClassMap = new HashMap<>();
  public EntityAggregateHandlerVerticle(
    final Class<T> entityAggregateClass,
    final List<CommandValidatorWrapper> commandValidators,
    final List<CommandBehaviourWrapper> commandBehaviours,
    final List<EventBehaviourWrapper> eventBehaviours,
    final List<QueryBehaviourWrapper> queryBehaviours,
    final List<ProjectionWrapper> projections,
    final EntityAggregateConfiguration entityAggregateConfiguration
  ) {
    this.entityAggregateClass = entityAggregateClass;
    this.commandValidators = commandValidators;
    this.commandBehaviours = commandBehaviours;
    this.eventBehaviours = eventBehaviours;
    this.projections = projections;
    this.queryBehaviours = queryBehaviours;
    this.eventJournalMapper = new EventJournalMapper(Objects.requireNonNull(entityAggregateConfiguration.eventJournalTable(), "Must define an event journal table"));
    this.snapshotRepositoryMapper = new AggregateSnapshotMapper(entityAggregateConfiguration.snapshotTable());
    this.rejectCommandMapper = new RejectedCommandMapper(Objects.requireNonNull(entityAggregateConfiguration.rejectCommandTable(), "Must define a table for rejected commands"));
    this.entityAggregateConfiguration = entityAggregateConfiguration;
  }

  @Override
  public Uni<Void> asyncStart() {
    LOGGER.info("Starting " + this.getClass().getSimpleName() + " " + context.deploymentID() + " configuration -> " + JsonObject.mapFrom(entityAggregateConfiguration).encodePrettily());
    this.repositoryHandler = RepositoryHandler.leasePool(config(), vertx);
    if (Boolean.TRUE.equals(entityAggregateConfiguration.useCache())) {
      this.entityAggregateCache = new EntityAggregateCache<>(
        vertx,
        entityAggregateClass,
        handlerAddress(),
        entityAggregateConfiguration.aggregateCacheTtlInMinutes()
      );
    }
    this.entityAggregateHandler = new EntityAggregateHandler<>(
      entityAggregateClass,
      commandValidators,
      eventBehaviours,
      commandBehaviours,
      queryBehaviours,
      projections,
      entityAggregateConfiguration,
      new Repository<>(eventJournalMapper, repositoryHandler),
      Boolean.TRUE.equals(entityAggregateConfiguration.snapshots()) ? new Repository<>(snapshotRepositoryMapper, repositoryHandler) : null,
      new Repository<>(rejectCommandMapper, repositoryHandler),
      entityAggregateCache,
      entityAggregateConfiguration.persistenceMode()
    );

    return vertx.eventBus().<JsonObject>consumer(handlerAddress())
      .handler(objectMessage -> {
          final var responseUni = switch (AggregateHandlerAction.valueOf(objectMessage.headers().get(ACTION))) {
            case LOAD -> entityAggregateHandler.load(objectMessage.body().mapTo(EntityAggregateKey.class));
            case COMMAND -> entityAggregateHandler.process(objectMessage.headers().get(CLASS_NAME), objectMessage.body());
            case COMPOSITE_COMMAND -> entityAggregateHandler.process(objectMessage.body().mapTo(CompositeCommandWrapper.class));
          };
          responseUni.subscribe()
            .with(
              objectMessage::reply,
              throwable -> {
                if (throwable instanceof VertxServiceException vertxServiceException) {
                  objectMessage.fail(vertxServiceException.error().errorCode(), JsonObject.mapFrom(vertxServiceException.error()).encode());
                } else {
                  LOGGER.error("Unexpected exception raised -> " + objectMessage.body(), throwable);
                  objectMessage.fail(500, JsonObject.mapFrom(new Error(throwable.getMessage(), throwable.getLocalizedMessage(), 500)).encode());
                }
              }
            );
        }
      )
      .exceptionHandler(this::droppedException)
      .completionHandler()
      .invoke(avoid -> entityAggregateCache.registerHandler());
  }

  @Override
  public String deploymentID() {
    return verticleUUID;
  }

  private String handlerAddress() {
    return entityAggregateClass.getName() + "." + context.deploymentID();
  }

  private void droppedException(final Throwable throwable) {
    LOGGER.error("[-- AggregateHandlerVerticle " + handlerAddress() + " had to drop the following exception --]", throwable);
  }

  private final String verticleUUID = UUID.randomUUID().toString();

  @Override
  public Uni<Void> asyncStop() {
    LOGGER.info("Stopping " + this.getClass().getSimpleName() + " deploymentID -> " + this.deploymentID());
    entityAggregateCache.unregisterHandler();
    return repositoryHandler.shutDown();
  }

}
