package io.vertx.eventx.task;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.eventx.sql.models.Query;
import io.vertx.eventx.sql.models.QueryOptions;

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
