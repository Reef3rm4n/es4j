package io.vertx.eventx.task;



import java.util.List;

public record TimerTaskConfiguration(
  LockLevel lockLevel,
  Long throttleInMs,
  Long interruptionBackOff,
  Long lockBackOffInMinutes,
  Long errorBackOffInMinutes,
  List<Class<? extends Throwable>> knownInterruptions
) {}
