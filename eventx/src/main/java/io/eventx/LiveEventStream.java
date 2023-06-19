package io.eventx;

import io.eventx.infrastructure.models.Event;
import io.smallrye.mutiny.Uni;

public interface LiveEventStream {
  Uni<Void> apply(Event event);
  default String tenant() {
    return "default";
  }

}
