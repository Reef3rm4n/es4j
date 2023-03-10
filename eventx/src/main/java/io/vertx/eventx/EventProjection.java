package io.vertx.eventx;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.objects.EventJournalFilter;

import java.util.List;

public interface EventProjection {

  Uni<Void> apply(List<Event> events);

  default EventJournalFilter filter() {
    return null;
  }

  default String tenantID() {
    return "default";
  }


}
