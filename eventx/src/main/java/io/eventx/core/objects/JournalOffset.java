package io.eventx.core.objects;


import io.eventx.infrastructure.models.Event;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.Comparator;
import java.util.List;

@RecordBuilder
public record JournalOffset(
  String consumer,
  String tenantId,
  Long idOffSet,
  Long eventVersionOffset
) {
  public JournalOffset updateOffset(List<io.eventx.infrastructure.models.Event> events) {
    final var eventIdOffset = events.stream().map(Event::journalOffset)
      .max(Comparator.naturalOrder())
      .orElseThrow();
    return new JournalOffset(consumer, tenantId, eventIdOffset, null);
  }
}
