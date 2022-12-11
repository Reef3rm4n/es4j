package io.vertx.skeleton.orm.mappers;

import io.vertx.skeleton.models.AggregateSnapshot;
import io.vertx.skeleton.models.EntityAggregateKey;
import io.vertx.skeleton.models.EmptyQuery;
import io.vertx.skeleton.orm.Constants;
import io.vertx.skeleton.orm.RepositoryMapper;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.mutiny.sqlclient.templates.TupleMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public  class AggregateSnapshotMapper implements RepositoryMapper<EntityAggregateKey, AggregateSnapshot, EmptyQuery> {
  private final String tableName;

  public AggregateSnapshotMapper(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public String table() {
    return tableName;
  }

  @Override
  public Set<String> insertColumns() {
    return Set.of(Constants.ENTITY_ID, Constants.EVENT_VERSION, Constants.STATE);
  }

  @Override
  public Set<String> updateColumns() {
    return Set.of(Constants.STATE, Constants.EVENT_VERSION);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(Constants.ENTITY_ID, Constants.TENANT);
  }


  @Override
  public RowMapper<AggregateSnapshot> rowMapper() {
    return RowMapper.newInstance(
      row -> new AggregateSnapshot(
        row.getString(Constants.ENTITY_ID),
        row.getLong(Constants.EVENT_VERSION),
        row.getJsonObject(Constants.STATE),
        from(row)
      )
    );
  }

  @Override
  public TupleMapper<AggregateSnapshot> tupleMapper() {
    return TupleMapper.mapper(
      entityEvent -> {
        Map<String, Object> parameters = entityEvent.persistedRecord().params();
        parameters.put(Constants.ENTITY_ID, entityEvent.entityId());
        parameters.put(Constants.EVENT_VERSION, entityEvent.eventVersion());
        parameters.put(Constants.STATE, entityEvent.state());
        return parameters;
      }
    );
  }

  @Override
  public TupleMapper<EntityAggregateKey> keyMapper() {
    return TupleMapper.mapper(
      key -> {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(Constants.ENTITY_ID, key.entityId());
        parameters.put(Constants.TENANT, key.tenant().generateString());
        return parameters;
      }
    );
  }

  @Override
  public Class<AggregateSnapshot> valueClass() {
    return AggregateSnapshot.class;
  }

  @Override
  public Class<EntityAggregateKey> keyClass() {
    return EntityAggregateKey.class;
  }

}

