package io.vertx.skeleton.orm.mappers;

import io.vertx.skeleton.models.ConsumerFailure;
import io.vertx.skeleton.models.EmptyQuery;
import io.vertx.skeleton.orm.RepositoryMapper;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.mutiny.sqlclient.templates.TupleMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.vertx.skeleton.orm.mappers.Constants.*;

public class ConsumerFailureMapper implements RepositoryMapper<String, ConsumerFailure, EmptyQuery> {

  private ConsumerFailureMapper(){}

  public static final ConsumerFailureMapper EVENT_CONSUMER_FAILURE_MAPPER = new ConsumerFailureMapper();
  public final RowMapper<ConsumerFailure> ROW_MAPPER = RowMapper.newInstance(
    row -> new ConsumerFailure(
      row.getString(CONSUMER),
      row.getInteger(ATTEMPTS),
      row.getJsonObject(EVENT),
      row.getJsonArray(ERRORS),
      tenantLessFrom(row)
    )
  );

  public static final TupleMapper<ConsumerFailure> TUPLE_MAPPER = TupleMapper.mapper(
    config -> {
      Map<String, Object> parameters = config.persistedRecord().tenantLessParams();
      parameters.put(CONSUMER, config.consumerName());
      parameters.put(ATTEMPTS, config.attempts());
      parameters.put(EVENT, config.event());
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
