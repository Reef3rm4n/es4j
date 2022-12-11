package io.vertx.skeleton.config;

import io.vertx.skeleton.orm.RepositoryMapper;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.mutiny.sqlclient.templates.TupleMapper;

import java.util.*;


public class ConfigurationRecordMapper implements RepositoryMapper<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> {

  public static final ConfigurationRecordMapper INSTANCE = new ConfigurationRecordMapper();
  private ConfigurationRecordMapper(){}
  public final RowMapper<ConfigurationRecord> ROW_MAPPER = RowMapper.newInstance(
    row -> new ConfigurationRecord(
      row.getString("name"),
      row.getString("class"),
      row.getJsonObject("data"),
      from(row)
    )
  );

  @Override
  public Class<ConfigurationRecord> valueClass() {
    return ConfigurationRecord.class;
  }

  public final TupleMapper<ConfigurationRecord> TUPLE_MAPPER = TupleMapper.mapper(
    config -> {
      Map<String, Object> parameters = config.persistedRecord().params();
      parameters.put("name", config.name());
      parameters.put("class", config.tClass());
      parameters.put("data", JsonObject.mapFrom(config.data()));
      return parameters;
    }
  );

  public static final TupleMapper<ConfigurationKey> KEY_MAPPER = TupleMapper.mapper(
    savedConfigurationKey -> {
      Map<String, Object> parameters = new HashMap<>();
      parameters.put("name", savedConfigurationKey.name());
      parameters.put("class", savedConfigurationKey.tClass());
      parameters.put("tenant", savedConfigurationKey.tenant().generateString());
      return parameters;
    }
  );

  @Override
  public List<Tuple2<String, List<?>>> queryFieldsColumn(final ConfigurationQuery queryFilter) {
    final var tupleList = new ArrayList<Tuple2<String, List<?>>>();
    tupleList.add(Tuple2.of("name", queryFilter.name()));
    tupleList.add(Tuple2.of("class", queryFilter.tClasses()));
    return tupleList;
  }

  @Override
  public String table() {
    return "configuration";
  }

  @Override
  public Set<String> insertColumns() {
    return Set.of("name", "class", "data");
  }

  @Override
  public Set<String> updateColumns() {
    return Set.of("data");
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of("name", "class", "tenant");
  }

  @Override
  public RowMapper<ConfigurationRecord> rowMapper() {
    return ROW_MAPPER;
  }

  @Override
  public TupleMapper<ConfigurationRecord> tupleMapper() {
    return TUPLE_MAPPER;
  }

  @Override
  public TupleMapper<ConfigurationKey> keyMapper() {
    return KEY_MAPPER;
  }

  @Override
  public Class<ConfigurationKey> keyClass() {
    return ConfigurationKey.class;
  }

}
