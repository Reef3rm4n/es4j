package io.vertx.skeleton.evs.mappers;

import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.objects.ConsumerFailure;
import io.vertx.skeleton.models.EmptyQuery;
import io.vertx.skeleton.orm.RepositoryMapper;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.mutiny.sqlclient.templates.TupleMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.vertx.skeleton.orm.Constants.ENTITY_ID;
import static io.vertx.skeleton.orm.mappers.Constants.*;

public class ConsumerFailureMapper implements RepositoryMapper<String, ConsumerFailure, EmptyQuery> {

  private ConsumerFailureMapper(){}

  private String tableName;
  public <T extends EntityAggregate>ConsumerFailureMapper(Class<T> entityAggregateClass) {
    this.tableName = entityAggregateClass.getSimpleName() + "_consumers_failures";
  }
  public final RowMapper<ConsumerFailure> ROW_MAPPER = RowMapper.newInstance(
    row -> new ConsumerFailure(
      row.getString(CONSUMER),
      row.getString(ENTITY_ID),
      row.getInteger(ATTEMPTS),
      row.getJsonArray(ERRORS),
      tenantLessFrom(row)
    )
  );

  public static final TupleMapper<ConsumerFailure> TUPLE_MAPPER = TupleMapper.mapper(
    config -> {
      Map<String, Object> parameters = config.persistedRecord().tenantLessParams();
      parameters.put(CONSUMER, config.consumer());
      parameters.put(ATTEMPTS, config.attempts());
      parameters.put(ERRORS, config.errors());
      return parameters;
    }
  );

  public static final TupleMapper<String> KEY_MAPPER = TupleMapper.mapper(
    consumerName -> {
      Map<String, Object> parameters = new HashMap<>();
      parameters.put(CONSUMER, consumerName);
      return parameters;
    }
  );


  @Override
  public String table() {
    return "event_consumers_failures";
  }

  @Override
  public Set<String> insertColumns() {
    return Set.of(CONSUMER, ATTEMPTS,  EVENT, ERRORS);
  }

  @Override
  public Set<String> updateColumns() {
    return Set.of(ATTEMPTS, ERRORS);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(CONSUMER);
  }

  @Override
  public RowMapper<ConsumerFailure> rowMapper() {
    return ROW_MAPPER;
  }

  @Override
  public TupleMapper<ConsumerFailure> tupleMapper() {
    return TUPLE_MAPPER;
  }

  @Override
  public TupleMapper<String> keyMapper() {
    return KEY_MAPPER;
  }

  @Override
  public Class<ConsumerFailure> valueClass() {
    return ConsumerFailure.class;
  }

  @Override
  public Class<String> keyClass() {
    return String.class;
  }
}
