package io.es4j.task;

import io.es4j.sql.models.Query;
import io.es4j.sql.models.QueryOptions;
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
