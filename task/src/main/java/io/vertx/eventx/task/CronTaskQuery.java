package io.vertx.eventx.task;

import io.vertx.eventx.sql.models.Query;
import io.vertx.eventx.sql.models.QueryOptions;

import java.time.Instant;
import java.util.List;

public record CronTaskQuery(
  List<String> taskClasses,
  Instant nextExecutionFrom,
  Instant nextExecutionTo,

  Instant lastExecutionFrom,
  Instant lastExecutionTo,
  QueryOptions options
) implements Query {
}
