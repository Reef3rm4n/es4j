package io.vertx.eventx;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.objects.EventJournalFilter;
import io.vertx.eventx.objects.PolledEvent;
import java.util.List;
import java.util.Optional;

public interface EventProjection {

  Uni<Void> apply(List<PolledEvent> events);

  default Optional<EventJournalFilter> filter() {
    return Optional.empty();
  }

  default String tenantID() {
    return null;
  }


}
