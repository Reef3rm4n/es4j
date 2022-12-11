package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.models.PersistedRecord;
import io.vertx.skeleton.models.RepositoryRecord;
import io.vertx.skeleton.models.Tenant;

public record QueueEventRecord(
  Tenant tenant,
  String resultClass,
  Object entry,
  Object result,
  Throwable throwable,
  QueueEventType queueEventType,
  PersistedRecord persistedRecord
) implements RepositoryRecord<QueueEventRecord> {
  @Override
  public QueueEventRecord with(PersistedRecord persistedRecord) {
    return new QueueEventRecord(
      tenant,
      resultClass,
      entry,
      result,
      throwable,
      queueEventType,
      persistedRecord
    );
  }
}
