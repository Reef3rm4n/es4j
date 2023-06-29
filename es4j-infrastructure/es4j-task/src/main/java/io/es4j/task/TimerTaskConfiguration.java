package io.es4j.task;



import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Duration;
import java.util.List;

@RecordBuilder
public record TimerTaskConfiguration(
  LockLevel lockLevel,
  Duration throttle,
  Duration interruptionBackOff,
  Duration lockBackOff,
  Duration errorBackOff,
  List<Class<? extends Throwable>> knownInterruptions
) {}
