package io.vertx.eventx.task;


import com.cronutils.model.definition.CronDefinition;

import java.util.List;

public record TimerTaskConfiguration(
  CronDefinition cronDefinition,
  LockLevel lockLevel,
  Long throttleInMs,
  Long interruptionBackOff,
  Long lockBackOffInMinutes,
  Long errorBackOffInMinutes,
  List<Class<? extends Throwable>> knownInterruptions
) {}
