package io.vertx.eventx.objects;

import io.vertx.eventx.Command;

import java.time.Instant;

public record ReplayUntil(
  String aggregateId,
  Long version,
  Instant date,
  CommandHeaders headers

) implements Command {
}
