package io.es4j.core.objects;

import java.time.Instant;


public record CommandOptions(
  Instant schedule,
  boolean simulate
) {
  public static CommandOptions defaultOptions() {
    return new CommandOptions(
      null,
      false
    );
  }
}
