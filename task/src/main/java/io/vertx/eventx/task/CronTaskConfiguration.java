package io.vertx.eventx.task;


import com.cronutils.model.Cron;

import java.time.LocalTime;
import java.util.List;

public record CronTaskConfiguration(
  Cron cron,
  LockLevel lockLevel,

  Integer timeoutInMinutes,
  List<Class<? extends Throwable>> knownInterruptions
) {

}
