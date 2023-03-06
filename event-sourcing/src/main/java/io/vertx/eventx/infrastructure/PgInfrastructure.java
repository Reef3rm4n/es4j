package io.vertx.eventx.infrastructure;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infrastructure.pg.PgCommandStore;
import io.vertx.eventx.infrastructure.pg.PgEventJournal;
import io.vertx.eventx.infrastructure.pg.PgSnapshotStore;
import io.vertx.eventx.infrastructure.pg.mappers.AggregateSnapshotMapper;
import io.vertx.eventx.infrastructure.pg.mappers.EventJournalMapper;
import io.vertx.eventx.infrastructure.pg.mappers.RejectedCommandMapper;
import io.vertx.eventx.infrastructure.vertx.VertxCache;
import io.vertx.eventx.sql.LiquibaseHandler;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.eventx.sql.misc.Constants;
import io.vertx.mutiny.core.Vertx;

import java.util.Map;

public final class PgInfrastructure<T extends Aggregate> {

  public static final String EVENT_SOURCING_XML = "event-sourcing.xml";
  private final Class<T> aggregateClass;
  private final RepositoryHandler repositoryHandler;
  private final AggregateCache<T> cache;
  private final CommandStore commandStore;
  private final EventJournal eventJournal;
  private final SnapshotStore snapshotStore;

  public PgInfrastructure(
    Class<T> aggregateClass,
    JsonObject coinfiguration,
    Vertx vertx
  ) {
    this.aggregateClass = aggregateClass;
    this.repositoryHandler = RepositoryHandler.leasePool(coinfiguration, vertx, aggregateClass);
    this.cache = new VertxCache<>(aggregateClass, repositoryHandler.vertx(), 30L);
    this.commandStore = new PgCommandStore(new Repository<>(RejectedCommandMapper.INSTANCE, repositoryHandler));
    this.eventJournal = new PgEventJournal(new Repository<>(EventJournalMapper.INSTANCE, repositoryHandler));
    this.snapshotStore = new PgSnapshotStore(new Repository<>(AggregateSnapshotMapper.INSTANCE, repositoryHandler));
  }
  public Uni<Void> start() {
    return LiquibaseHandler.liquibaseString(
      repositoryHandler,
      EVENT_SOURCING_XML,
      Map.of(Constants.SCHEMA, RepositoryHandler.camelToSnake(aggregateClass.getSimpleName()))
    );
  }

  public Uni<Void> stop() {
    return repositoryHandler.close();
  }

  public Class<T> aggregateClass() {
    return aggregateClass;
  }

  public RepositoryHandler repositoryHandler() {
    return repositoryHandler;
  }

  public AggregateCache<T> cache() {
    return cache;
  }

  public CommandStore commandStore() {
    return commandStore;
  }

  public EventJournal eventJournal() {
    return eventJournal;
  }

  public SnapshotStore snapshotStore() {
    return snapshotStore;
  }


}
