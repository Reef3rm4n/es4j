package io.es4j;

import io.es4j.infrastructure.models.Event;
import io.smallrye.mutiny.Uni;

public interface LiveEventProjection {
  Uni<Void> apply(Event event);
  default String tenant() {
    return "default";
  }

}
