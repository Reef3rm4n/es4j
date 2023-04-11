package io.vertx.eventx.objects;

import java.time.Instant;

public record CommandOptions(
  Instant schedule,
  String cron,
  boolean simulate
) {
  public static CommandOptions defaultOptions() {
    return new CommandOptions(
      null,
      null,
      false
    );
  }
}
