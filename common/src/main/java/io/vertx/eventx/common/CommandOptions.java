package io.vertx.eventx.common;

import java.time.Instant;

public record CommandOptions(
  Instant schedule,
  String cron,
  Boolean simulate
) {
}
