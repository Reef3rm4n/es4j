package io.es4j.task;



import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@RecordBuilder
public record TimerTaskConfiguration(
  LockLevel lockLevel,
  Duration throttle,
  Duration lockBackOff,
  Duration errorBackOff,
  Optional<Class<? extends Throwable>> finalInterruption
) {}
