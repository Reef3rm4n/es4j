package io.es4j.core.objects;

import io.es4j.Command;

import java.time.Instant;

public record LoadAggregate(
  String aggregateId,
  String tenant,
  Long versionTo,
  Instant dateTo

) implements Command {
}
