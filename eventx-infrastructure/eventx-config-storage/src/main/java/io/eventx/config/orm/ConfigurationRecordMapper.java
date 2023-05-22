package io.eventx.config.orm;


import io.eventx.sql.generator.filters.QueryBuilder;
import io.eventx.sql.RecordMapper;
import io.eventx.sql.models.QueryFilters;
import io.vertx.sqlclient.Row;

import java.util.*;


public class ConfigurationRecordMapper implements RecordMapper<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> {

  public static final ConfigurationRecordMapper INSTANCE = new ConfigurationRecordMapper();
  public static final String NAME = "name";
  public static final String CLASS = "class";
  public static final String DATA = "data";
  public static final String CONFIGURATION = "configuration";
  public static final String DESCRIPTION = "description";
  public static final String REVISION = "revision";
  public static final String ACTIVE = "active";

  private ConfigurationRecordMapper() {
  }

  @Override
  public String table() {
    return CONFIGURATION;
  }

  @Override
  public Set<String> columns() {
    return Set.of(NAME, CLASS, DATA, REVISION, DESCRIPTION, ACTIVE);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(NAME, CLASS, REVISION);
  }

  @Override
  public ConfigurationRecord rowMapper(Row row) {
    return new ConfigurationRecord(
      row.getString(NAME),
      row.getString(DESCRIPTION),
      row.getInteger(REVISION),
      row.getString(CLASS),
      row.getJsonObject(DATA),
      row.getBoolean(ACTIVE),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, ConfigurationRecord actualRecord) {
    params.put(NAME, actualRecord.name());
    params.put(CLASS, actualRecord.tClass());
    params.put(DATA, actualRecord.data());
    params.put(ACTIVE, actualRecord.active());
    params.put(REVISION, actualRecord.revision());
    params.put(DESCRIPTION, actualRecord.description());
  }

  @Override
  public void keyParams(Map<String, Object> params, ConfigurationKey key) {
    params.put(NAME, key.name());
    params.put(CLASS, key.tClass());
    params.put(REVISION, key.revision());
  }

  @Override
  public void queryBuilder(ConfigurationQuery query, QueryBuilder builder) {
    builder
      .iLike(
        new QueryFilters<>(String.class)
        .filterColumn(NAME)
        .filterParams(query.name())
      )
      .iLike(
        new QueryFilters<>(String.class)
        .filterColumn(CLASS)
        .filterParams(query.tClasses())
      );
  }
}
