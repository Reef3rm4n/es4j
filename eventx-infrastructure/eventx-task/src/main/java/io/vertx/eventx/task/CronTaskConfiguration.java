package io.vertx.eventx.task;


import com.cronutils.model.Cron;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.LocalTime;
import java.util.List;
@RecordBuilder
public record CronTaskConfiguration(
  Cron cron,
  Integer priority,
  List<Class<? extends Throwable>> knownInterruptions
) {

}
