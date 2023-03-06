package io.vertx.eventx.infrastructure;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Aggregate;
import io.vertx.mutiny.core.Vertx;

public interface Infra {


  <T extends Aggregate> Uni<Void> start(Class<T> aggregateClass, Vertx vertx, JsonObject configuration);

  Uni<Void> stop();

  <T extends Aggregate> AggregateCache<T> cache();

  CommandStore commandStore();

  EventJournal eventJournal();

  SnapshotStore snapshotStore();
}
