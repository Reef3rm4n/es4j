package io.eventx.task;

import io.eventx.sql.models.Query;
import io.eventx.sql.models.QueryOptions;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Instant;
import java.util.List;

@RecordBuilder
public record CronTaskQuery(
  List<String> taskClasses,
  Instant nextExecutionFrom,
  Instant nextExecutionTo,

  Instant lastExecutionFrom,
  Instant lastExecutionTo,
  QueryOptions options
) implements Query {
}
