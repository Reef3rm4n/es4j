package io.es4j.task;

import java.time.Instant;

public record CronTaskMessage(
  String taskClass,
  Instant lastRun
) {
}
