package io.vertx.skeleton.evs.objects;

import io.smallrye.mutiny.Uni;
import io.vertx.skeleton.evs.Projection;

import java.util.List;

public record ProjectionWrapper<T>(
  Projection<T> projection,
  Class<T> entityAggregateClass
)  {
  public  Uni<Void> update(Object state, List<Object> events) {
    return uniVoid(state, events);
  }

  private Uni<Void> uniVoid(Object state, List<Object> eventClasses) {
    return projection.update(entityAggregateClass.cast(state), eventClasses);
  }
}
