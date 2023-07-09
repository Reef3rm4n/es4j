package io.es4j.core.objects;


import io.es4j.infrastructure.models.Event;
import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

@RecordBuilder
public record Offset(
  String consumer,
  String tenantId,
  Long idOffSet,
  Long eventVersionOffset,
  Instant lastUpdate,
  Instant creationDate
) implements Serializable, Shareable {
  public Offset updateOffset(List<io.es4j.infrastructure.models.Event> events) {
    final var eventIdOffset = events.stream().map(Event::journalOffset)
      .max(Comparator.naturalOrder())
      .orElseThrow();
    return new Offset(consumer, tenantId, eventIdOffset, null, Instant.now(), creationDate);
  }
}
