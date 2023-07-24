package io.es4j.infrastructure.pgbroker.mappers;


import io.es4j.infrastructure.pgbroker.models.BrokerPartitionRecord;
import io.es4j.infrastructure.pgbroker.models.PartitionKey;
import io.es4j.infrastructure.pgbroker.models.PartitionQuery;
import io.es4j.sql.RecordMapper;
import io.es4j.sql.generator.filters.QueryBuilder;
import io.es4j.sql.models.QueryFilters;
import io.vertx.sqlclient.Row;

import java.util.Map;
import java.util.Set;


public class BrokerPartitionMapper implements RecordMapper<PartitionKey, BrokerPartitionRecord, PartitionQuery> {

    private static final String PARTITION_ID = "partition_id";
    private static final String LOCKED = "locked";
    private static final String TABLE_NAME = "message_broker_partition";

    public static final BrokerPartitionMapper INSTANCE = new BrokerPartitionMapper();
    public static final String DEPLOYMENT_ID = "verticle_id";

    private BrokerPartitionMapper() {
    }

    @Override
    public String table() {
        return TABLE_NAME;
    }

    @Override
    public Set<String> columns() {
        return Set.of(PARTITION_ID, LOCKED, DEPLOYMENT_ID);

    }

    @Override
    public Set<String> keyColumns() {
        return Set.of(PARTITION_ID);
    }

    @Override
    public BrokerPartitionRecord rowMapper(Row row) {
        return new BrokerPartitionRecord(
            row.getString(PARTITION_ID),
            row.getString(DEPLOYMENT_ID),
            row.getBoolean(LOCKED),
            baseRecord(row)
        );
    }
    @Override
    public void params(Map<String, Object> params, BrokerPartitionRecord actualRecord) {
        params.put(PARTITION_ID, actualRecord.partitionId());
        params.put(LOCKED, actualRecord.locked());
        params.put(DEPLOYMENT_ID, actualRecord.deploymentId());
    }

    @Override
    public void keyParams(Map<String, Object> params, PartitionKey key) {
        params.put(PARTITION_ID, key.partitionId());
    }

    @Override
    public void queryBuilder(PartitionQuery query, QueryBuilder builder) {
        builder.eq(
            new QueryFilters<>(Long.class)
                .filterColumn(PARTITION_ID)
                .filterParams(query.lockIds())
        );
    }

}
