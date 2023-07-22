package io.es4j.infrastructure.pgbroker.models;


import io.es4j.sql.models.RepositoryRecordKey;
import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record PartitionKey(
  String partitionId
) implements RepositoryRecordKey {
}
