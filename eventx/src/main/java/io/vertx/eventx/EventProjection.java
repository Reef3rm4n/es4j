package io.vertx.eventx;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.objects.EventJournalFilter;

import javax.swing.text.html.Option;
import java.util.List;
import java.util.Optional;

public interface EventProjection {

  Uni<Void> apply(List<Event> events);

  default Optional<EventJournalFilter> filter() {
    return Optional.empty();
  }

  default String tenantID() {
    return "default";
  }


}
