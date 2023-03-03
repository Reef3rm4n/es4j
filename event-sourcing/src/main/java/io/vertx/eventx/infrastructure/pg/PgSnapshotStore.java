package io.vertx.eventx.infrastructure.pg;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infrastructure.SnapshotStore;
import io.vertx.eventx.infrastructure.models.AggregateKey;
import io.vertx.eventx.infrastructure.pg.models.AggregateRecordKey;
import io.vertx.eventx.infrastructure.pg.models.AggregateSnapshotRecord;
import io.vertx.eventx.objects.EntityState;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.models.EmptyQuery;

public class PgSnapshotStore implements SnapshotStore {
  private final Repository<AggregateRecordKey, AggregateSnapshotRecord, EmptyQuery> snapshots;

  public PgSnapshotStore(Repository<AggregateRecordKey, AggregateSnapshotRecord, EmptyQuery> snapshots) {
    this.snapshots = snapshots;
  }

  @Override
  public <T extends Aggregate> Uni<EntityState<T>> get(AggregateKey<T> key) {
    return null;
  }

  @Override
  public <T extends Aggregate> Uni<Void> add(EntityState<T> value) {
    return null;
  }

  @Override
  public <T extends Aggregate> Uni<Void> update(EntityState<T> aggregate) {
    return null;
  }

}
