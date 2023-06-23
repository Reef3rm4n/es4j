package io.eventx;

import io.eventx.core.objects.AggregateState;
import io.smallrye.mutiny.Uni;

public interface LiveStateProjection<T extends Aggregate> {

  Uni<Void> update(AggregateState<T> currentState);

  default String tenant() {
    return "default";
  }


}
