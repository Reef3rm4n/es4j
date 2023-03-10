package io.vertx.eventx.config.orm;


import io.vertx.eventx.sql.RecordMapper;
import io.vertx.eventx.sql.generator.filters.QueryBuilder;
import io.vertx.sqlclient.Row;

import java.util.*;


public class ConfigurationRecordMapper implements RecordMapper<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> {

  public static final ConfigurationRecordMapper INSTANCE = new ConfigurationRecordMapper();
  public static final String NAME = "name";
  public static final String CLASS = "class";
  public static final String DATA = "data";

  private ConfigurationRecordMapper(){}

  @Override
  public String table() {
    return null;
  }

  @Override
  public Set<String> columns() {
    return null;
  }

  @Override
  public Set<String> keyColumns() {
    return null;
  }

  @Override
  public ConfigurationRecord rowMapper(Row row) {
    return null;
  }

  @Override
  public void params(Map<String, Object> params, ConfigurationRecord actualRecord) {

  }

  @Override
  public void keyParams(Map<String, Object> params, ConfigurationKey key) {

  }

  @Override
  public void queryBuilder(ConfigurationQuery query, QueryBuilder builder) {

  }
}
