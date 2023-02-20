package io.vertx.skeleton.sql.models;

import io.vertx.skeleton.sql.RecordMapper;
import io.vertx.skeleton.sql.generator.filters.QueryBuilder;
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
  public void params(Map<String, Object> params, TestModel record) {
    params.put(TEXT_FIELD, record.textField());
    params.put(TIMESTAMP_FIELD, LocalDateTime.ofInstant(record.timeStampField(), ZoneOffset.UTC));
    params.put(JSON_FIELD, record.jsonObjectField());
    params.put(LONG_FIELD, record.longField());
    params.put(INTEGER_FIELD, record.integerField());
  }

  @Override
  public void keyParams(Map<String, Object> params, TestModelKey key) {
    params.put(TEXT_FIELD, key.textField());
  }

  @Override
  public void queryBuilder(TestModelQuery query, QueryBuilder builder) {
    builder
      .iLike(
        new QueryParams<>(String.class)
          .setColumn(TEXT_FIELD)
          .setParams(query.textFields())
      )
      .from(
        new QueryParam<>(Instant.class)
          .setColumn(TIMESTAMP_FIELD)
          .setParam(query.timestampFieldFrom())
      )
      .to(
        new QueryParam<>(Instant.class)
          .setColumn(TIMESTAMP_FIELD)
          .setParam(query.timestampFieldTo())
      )
      .eq(
        new QueryParams<>(Long.class)
          .setColumn(LONG_FIELD)
          .setParams(query.longEqField())
      )
      .from(
        new QueryParam<>(Long.class)
          .setColumn(LONG_FIELD)
          .setParam(query.longFieldFrom())
      )
      .to(
        new QueryParam<>(Long.class)
          .setColumn(LONG_FIELD)
          .setParam(query.longEqField())
      )
      .jsonILike(new JsonQueryParams<>(String.class)
        .setColumn(JSON_FIELD)
        .addJsonFields("person").addJsonFields("details").addJsonFields("name")
        .setParams(query.names())
      );
  }


}
