package io.es4j.infrastructure;


import io.es4j.Deployment;
import io.es4j.core.objects.Offset;
import io.es4j.infrastructure.models.OffsetFilter;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.es4j.core.objects.OffsetKey;
import io.vertx.mutiny.core.Vertx;

import java.util.List;

public interface OffsetStore {

  Uni<Offset> put(Offset offset);
  Uni<Offset> get(OffsetKey journalOffset);
  Uni<Offset> reset(Offset offset);

  Uni<List<Offset>> projections(OffsetFilter offsetFilter);
  Uni<Void> stop();
  void start(Deployment deployment, Vertx vertx, JsonObject configuration);
  Uni<Void> setup(Deployment deployment, Vertx vertx, JsonObject configuration);
}
