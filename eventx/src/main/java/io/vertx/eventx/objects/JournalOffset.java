package io.vertx.eventx.objects;


import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.eventx.infrastructure.models.Event;

import java.util.Comparator;
import java.util.List;

@RecordBuilder
public record JournalOffset(
  String consumer,
  String tenantId,
  Long idOffSet,
  Long eventVersionOffset
) {
  public JournalOffset updateOffset(List<Event> events) {
    final var eventIdOffset = events.stream().map(Event::journalOffset)
      .max(Comparator.naturalOrder())
      .orElseThrow();
    return new JournalOffset(consumer, tenantId, eventIdOffset, null);
  }
}
