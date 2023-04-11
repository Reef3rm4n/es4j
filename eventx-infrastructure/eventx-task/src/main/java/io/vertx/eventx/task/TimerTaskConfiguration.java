package io.vertx.eventx.task;



import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;

@RecordBuilder
public record TimerTaskConfiguration(
  LockLevel lockLevel,
  Long throttleInMs,
  Long interruptionBackOff,
  Long lockBackOffInMinutes,
  Long errorBackOffInMinutes,
  List<Class<? extends Throwable>> knownInterruptions
) {}
