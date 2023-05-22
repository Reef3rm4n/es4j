package io.eventx.task;

import io.eventx.sql.models.RepositoryRecordKey;

public record CronTaskKey(
  String taskClass
)implements RepositoryRecordKey {
}
