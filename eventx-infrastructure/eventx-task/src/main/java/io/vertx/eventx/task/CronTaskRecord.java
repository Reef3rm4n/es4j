package io.vertx.eventx.task;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.eventx.sql.models.RepositoryRecord;
import io.vertx.eventx.sql.models.BaseRecord;

import java.time.Instant;

@RecordBuilder
public record CronTaskRecord(
  String taskClass,
  Instant lastExecutionTime,
  Instant nextExecutionTime,
  BaseRecord baseRecord
) implements RepositoryRecord<CronTaskRecord> {
  @Override
  public CronTaskRecord with(BaseRecord baseRecord) {
    return new CronTaskRecord(taskClass, lastExecutionTime, nextExecutionTime, baseRecord);
  }

  public CronTaskRecord newExecutionTime(Instant executionTime) {
    return new CronTaskRecord(taskClass, nextExecutionTime, executionTime, baseRecord);

  }
}
