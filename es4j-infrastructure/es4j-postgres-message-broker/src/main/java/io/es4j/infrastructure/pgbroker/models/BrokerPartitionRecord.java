package io.es4j.infrastructure.pgbroker.models;


import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;
import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record BrokerPartitionRecord(
    String partitionId,
    String deploymentId,
    Boolean locked,
    BaseRecord baseRecord
) implements RepositoryRecord<BrokerPartitionRecord> {

    @Override
    public BrokerPartitionRecord with(BaseRecord baseRecord) {
        return BrokerPartitionRecordBuilder.builder(this)
          .baseRecord(baseRecord)
          .build();
    }

    public BrokerPartitionRecord release() {
      return BrokerPartitionRecordBuilder.builder(this)
        .locked(false)
        .deploymentId(null)
        .build();
    }
}
