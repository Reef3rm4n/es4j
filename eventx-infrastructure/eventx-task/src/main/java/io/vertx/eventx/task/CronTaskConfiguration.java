package io.vertx.eventx.task;


import com.cronutils.model.Cron;

import java.time.LocalTime;
import java.util.List;

public record CronTaskConfiguration(
  Cron cron,
  Integer priority,
  List<Class<? extends Throwable>> knownInterruptions
) {

}
