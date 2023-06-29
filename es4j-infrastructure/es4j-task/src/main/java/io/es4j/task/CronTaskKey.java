package io.es4j.task;

import io.es4j.sql.models.RepositoryRecordKey;

public record CronTaskKey(
  String taskClass
)implements RepositoryRecordKey {
}
