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
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.mutiny.core.Vertx;

public record Infrastructure<T extends Aggregate>(
  AggregateCache<T> cache,
  CommandStore commandStore,
  EventJournal eventJournal,
  SnapshotStore snapshotStore
) {


  public static <A extends Aggregate> Infrastructure<A> bootstrap(Class<A> aggregateClass, Vertx vertx, JsonObject configuration) {
    final var repositoryHandler = RepositoryHandler.leasePool(configuration, vertx, aggregateClass);
    return new Infrastructure<>(
      new VertxCache<>(aggregateClass, vertx, 30L),
      new PgCommandStore(new Repository<>(RejectedCommandMapper.INSTANCE, repositoryHandler)),
      new PgEventJournal(new Repository<>(EventJournalMapper.INSTANCE, repositoryHandler)),
      new PgSnapshotStore(new Repository<>(AggregateSnapshotMapper.INSTANCE, repositoryHandler))
    );
  }

  public Uni<Void> stop() {
    return Uni.createFrom().voidItem();
  }


}
