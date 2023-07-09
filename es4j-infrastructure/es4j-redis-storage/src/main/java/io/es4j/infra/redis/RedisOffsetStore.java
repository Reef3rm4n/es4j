package io.es4j.infra.redis;

import com.google.auto.service.AutoService;
import io.es4j.Aggregate;
import io.es4j.core.objects.Offset;
import io.es4j.core.objects.OffsetKey;
import io.es4j.infrastructure.OffsetStore;
import io.es4j.infrastructure.models.OffsetFilter;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.List;


@AutoService(OffsetStore.class)
public class RedisOffsetStore implements OffsetStore {
  @Override
  public Uni<Offset> put(Offset offset) {
    return null;
  }

  @Override
  public Uni<Offset> get(OffsetKey journalOffset) {
    return null;
  }

  @Override
  public Uni<Offset> reset(Offset offset) {
    return null;
  }

  @Override
  public Uni<List<Offset>> projections(OffsetFilter offsetFilter) {
    return null;
  }

  @Override
  public Uni<Void> stop() {
    return null;
  }

  @Override
  public void start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration) {

  }

  @Override
  public Uni<Void> setup(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration) {
    return null;
  }

}
