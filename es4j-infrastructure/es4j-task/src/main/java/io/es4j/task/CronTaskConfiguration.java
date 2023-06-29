package io.es4j.task;


import com.cronutils.model.Cron;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;

@RecordBuilder
public record CronTaskConfiguration(
  Cron cron,
  LockLevel lockLevel,
  List<Class<? extends Throwable>> knownInterruptions
) {

}
