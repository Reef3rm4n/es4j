package io.vertx.eventx.task;

import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record CronTaskKey(
  String taskClass
)implements RepositoryRecordKey {
}
