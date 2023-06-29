package io.es4j.task;

import io.es4j.sql.generator.filters.QueryBuilder;
import io.es4j.sql.models.QueryFilter;
import io.es4j.sql.models.QueryFilters;
import io.es4j.sql.RecordMapper;
import io.vertx.sqlclient.Row;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;

public class CronTaskMapper implements RecordMapper<CronTaskKey,CronTaskRecord,CronTaskQuery> {

  public static final String CRON_TASKS = "cron_tasks";
  public static final String LAST_EXECUTION = "last_execution";
  public static final String NEXT_EXECUTION = "next_execution";
  public static final String TASK_CLASS = "task_class";

  private CronTaskMapper() {}

  public static final CronTaskMapper INSTANCE = new CronTaskMapper();

  @Override
  public String table() {
    return CRON_TASKS;
  }

  @Override
  public Set<String> columns() {
    return Set.of(TASK_CLASS, NEXT_EXECUTION, LAST_EXECUTION);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(TASK_CLASS);
  }

  @Override
  public CronTaskRecord rowMapper(Row row) {
    return new CronTaskRecord(
      row.getString(TASK_CLASS),
      row.getLocalDateTime(LAST_EXECUTION).toInstant(ZoneOffset.UTC),
      row.getLocalDateTime(NEXT_EXECUTION).toInstant(ZoneOffset.UTC),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, CronTaskRecord actualRecord) {
    params.put(TASK_CLASS, actualRecord.taskClass());
    params.put(LAST_EXECUTION, LocalDateTime.ofInstant(actualRecord.lastExecutionTime(),ZoneOffset.UTC));
    params.put(NEXT_EXECUTION, LocalDateTime.ofInstant(actualRecord.nextExecutionTime(), ZoneOffset.UTC));
  }

  @Override
  public void keyParams(Map<String, Object> params, CronTaskKey key) {
    params.put(TASK_CLASS, key.taskClass());
  }

  @Override
  public void queryBuilder(CronTaskQuery query, QueryBuilder builder) {
    builder
      .iLike(
        new QueryFilters<>(String.class)
        .filterColumn(TASK_CLASS)
        .filterParams(query.taskClasses())
      )
      .from(
        new QueryFilter<>(Instant.class)
        .filterColumn(NEXT_EXECUTION)
        .filterParam(query.nextExecutionFrom())
      )
      .to(
        new QueryFilter<>(Instant.class)
        .filterColumn(NEXT_EXECUTION)
        .filterParam(query.nextExecutionTo())
      )
      .from(
        new QueryFilter<>(Instant.class)
        .filterColumn(LAST_EXECUTION)
        .filterParam(query.lastExecutionFrom())
      )
      .to(
        new QueryFilter<>(Instant.class)
        .filterColumn(LAST_EXECUTION)
        .filterParam(query.lastExecutionTo())
      );
  }
}
