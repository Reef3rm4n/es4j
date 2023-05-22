package io.eventx.core.objects;

import io.eventx.Command;

import java.time.Instant;

public record LoadAggregate(
  String aggregateId,
  Long versionTo,
  Instant dateTo,
  CommandHeaders headers

) implements Command {
}
