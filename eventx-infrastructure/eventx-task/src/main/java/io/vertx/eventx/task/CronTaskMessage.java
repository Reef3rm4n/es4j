package io.vertx.eventx.task;

import java.time.Instant;

public record CronTaskMessage(
  String taskClass,
  Instant lastRun
) {
}
