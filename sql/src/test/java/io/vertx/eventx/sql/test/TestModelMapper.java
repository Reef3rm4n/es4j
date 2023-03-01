package io.vertx.eventx.sql.test;

import io.vertx.eventx.sql.RecordMapper;
import io.vertx.eventx.sql.generator.filters.QueryBuilder;
import io.vertx.eventx.sql.models.JsonQueryFilter;
import io.vertx.eventx.sql.models.QueryFilter;
import io.vertx.eventx.sql.models.QueryFilters;
import io.vertx.sqlclient.Row;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;

public class TestModelMapper implements RecordMapper<TestModelKey, TestModel, TestModelQuery> {

  public static final String TEXT_FIELD = "text_field";
  public static final String TIMESTAMP_FIELD = "timestamp_field";
  public static final String JSON_FIELD = "json_field";
  public static final String LONG_FIELD = "long_field";
  public static final String INTEGER_FIELD = "integer_field";
  public static final String TEST_MODEL_TABLE = "test_model_table";

  @Override
  public String table() {
    return TEST_MODEL_TABLE;
  }

  @Override
  public Set<String> columns() {
    return Set.of(TEXT_FIELD, TIMESTAMP_FIELD, JSON_FIELD, LONG_FIELD, INTEGER_FIELD);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(TEXT_FIELD);
  }

  @Override
  public TestModel rowMapper(Row row) {
    return new TestModel(
      row.getString(TEXT_FIELD),
      row.getLocalDateTime(TIMESTAMP_FIELD).toInstant(ZoneOffset.UTC),
      row.getJsonObject(JSON_FIELD),
      row.getLong(LONG_FIELD),
      row.getInteger(INTEGER_FIELD),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, TestModel actualRecord) {
    params.put(TEXT_FIELD, actualRecord.textField());
    params.put(TIMESTAMP_FIELD, LocalDateTime.ofInstant(actualRecord.timeStampField(), ZoneOffset.UTC));
    params.put(JSON_FIELD, actualRecord.jsonObjectField());
    params.put(LONG_FIELD, actualRecord.longField());
    params.put(INTEGER_FIELD, actualRecord.integerField());
  }

  @Override
  public void keyParams(Map<String, Object> params, TestModelKey key) {
    params.put(TEXT_FIELD, key.textField());
  }

  @Override
  public void queryBuilder(TestModelQuery query, QueryBuilder builder) {
    builder
      .iLike(
        new QueryFilters<>(String.class)
          .filterColumn(TEXT_FIELD)
          .filterParams(query.textFields())
      )
      .from(
        new QueryFilter<>(Instant.class)
          .filterColumn(TIMESTAMP_FIELD)
          .filterParam(query.timestampFieldFrom())
      )
      .to(
        new QueryFilter<>(Instant.class)
          .filterColumn(TIMESTAMP_FIELD)
          .filterParam(query.timestampFieldTo())
      )
      .eq(
        new QueryFilters<>(Long.class)
          .filterColumn(LONG_FIELD)
          .filterParams(query.longEqField())
      )
      .from(
        new QueryFilter<>(Long.class)
          .filterColumn(LONG_FIELD)
          .filterParam(query.longFieldFrom())
      )
      .to(
        new QueryFilter<>(Long.class)
          .filterColumn(LONG_FIELD)
          .filterParam(query.longEqField())
      )
      .jsonILike(new JsonQueryFilter<>(String.class)
        .filterColumn(JSON_FIELD)
        .navJson("person").navJson("details").navJson("name")
        .filterParams(query.names())
      );
  }


}
