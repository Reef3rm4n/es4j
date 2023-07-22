package io.es4j.infrastructure.pgbroker.models;


import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;
import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record MessagePartition(
    String partitionId,
    String deploymentId,
    Boolean locked,
    BaseRecord baseRecord
) implements RepositoryRecord<MessagePartition> {

    @Override
    public MessagePartition with(BaseRecord baseRecord) {
        return new MessagePartition(partitionId, deploymentId, locked, baseRecord);
    }

    public MessagePartition release() {
        return new MessagePartition(partitionId, deploymentId, false, baseRecord);
    }
}
