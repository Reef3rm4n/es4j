

/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.vertx.skeleton.orm;


import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.mutiny.sqlclient.templates.TupleMapper;
import io.vertx.skeleton.models.*;
import io.vertx.sqlclient.Row;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;


public interface RepositoryMapper<K, V extends RepositoryRecord<V>, Q extends Query> {

  Logger logger = LoggerFactory.getLogger(RepositoryMapper.class);

  String table();

  Set<String> insertColumns();

  Set<String> updateColumns();

  Set<String> keyColumns();

  RowMapper<V> rowMapper();

  RowMapper<Integer> ROW_MAPPER = RowMapper.newInstance(
    row -> row.getInteger("count")
  );

  default RowMapper<Integer> countMapper() {
    return ROW_MAPPER;
  }

  TupleMapper<V> tupleMapper();

  TupleMapper<K> keyMapper();

  Class<V> valueClass();

  Class<K> keyClass();

  default PersistedRecord from(Row row) {
    return new PersistedRecord(row.getLong("id"), Tenant.fromString(row.getString("tenant")), row.getInteger("version"), row.getLocalDateTime("creation_date").toInstant(ZoneOffset.UTC),
      row.getLocalDateTime("last_update").toInstant(ZoneOffset.UTC)
    );
  }

  default PersistedRecord tenantLessFrom(Row row) {
    return new PersistedRecord(row.getLong("id"), null, row.getInteger("version"), row.getLocalDateTime("creation_date").toInstant(ZoneOffset.UTC),
      row.getLocalDateTime("last_update").toInstant(ZoneOffset.UTC)
    );
  }

  default List<Tuple2<String, List<?>>> queryFieldsColumn(Q queryFilter) {
    return Collections.emptyList();
  }


  default void queryExtraFilters(Q queryFilter, StringJoiner stringJoiner) {

  }

  default void dateFrom(StringJoiner stringJoiner, Instant dateFrom, String column) {
    if (dateFrom != null) {
      stringJoiner.add(" " + column + " >= '" + LocalDateTime.ofInstant(dateFrom, ZoneOffset.UTC) + "'::timestamp ");
    }
  }

  default void dateTo(StringJoiner stringJoiner, Instant dateFrom, String column) {
    if (dateFrom != null) {
      stringJoiner.add(" " + column + " <= '" + LocalDateTime.ofInstant(dateFrom, ZoneOffset.UTC) + "'::timestamp ");
    }
  }

  default void arrayIn(StringJoiner stringJoiner, List<String> list, String column) {
    if (list != null && !list.isEmpty()) {
      final var searchArray = new StringJoiner(",");
      list.forEach(tag -> searchArray.add("'" + tag + "'::varchar"));
      stringJoiner.add(" array[" + searchArray + "] <@ " + column + " ");
    }
  }

  default void from(StringJoiner joiner, Integer integer, String column) {
    if (integer != null) {
      joiner.add(" " + column + " >= " + integer + " ");
    }

  }

  default void to(StringJoiner joiner, Integer integer, String column) {
    if (integer != null) {
      joiner.add(" " + column + " <= " + integer + " ");
    }
  }

  default void from(StringJoiner joiner, Long aLong, String column) {
    if (aLong != null) {
      joiner.add(" " + column + " >= " + aLong + " ");
    }

  }

  default void to(StringJoiner joiner, Long aLong, String column) {
    if (aLong != null) {
      joiner.add(" " + column + " <= " + aLong + " ");
    }

  }


  default String selectById() {
    return "select * from " + table() + " where id = #{id}";
  }

  default String selectByKey() {
    final var joiner1 = new StringJoiner(" and ");
    keyColumns().forEach(c -> joiner1.add(c + " = #{" + c + "}"));
    String query = "select * from " + table() + " where " + joiner1 + " ;";
    return query;
  }

  default String selectAll() {
    return "select * from " + table() + ";";
  }

  default String deleteByKey() {
    final var joiner1 = new StringJoiner(" and ");
    keyColumns().forEach(c -> joiner1.add(c + " = #{" + c + "}"));
    String query = "delete from " + table() + " where " + joiner1 + " returning id;";
    return query;
  }

  default String deleteById() {
    return "delete from " + table() + " where id = #{id} returning id";
  }


  default String updateByKey() {
    final var keyJoiner = new StringJoiner(" and ");
    keyColumns().forEach(c -> keyJoiner.add(c + " = #{" + c + "}"));
    final var joiner1 = new StringJoiner(", ");
    updateColumns().forEach(c -> joiner1.add(c + " = #{" + c + "}"));
    String query = "update " + table() + " set last_update = current_timestamp, version = version + 1, " + joiner1 + " where " + keyJoiner + " returning *;";
    return query;
  }

  default String updateById() {
    final var joiner1 = new StringJoiner(", ");
    updateColumns().forEach(c -> joiner1.add(c + " = #{" + c + "}"));
    String query = "update " + table() + " set last_update = current_timestamp, version = #{version} + 1, " + joiner1 + " where id = #{id} and version = #{version} returning version;";
    return query;
  }

  default String insert() {
    final var joiner1 = new StringJoiner(", ");
    final var joiner2 = new StringJoiner(", ");
    insertColumns().forEach(joiner1::add);
    insertColumns().forEach(c -> joiner2.add("#{" + c + "}"));
    String query = "insert into " + table() + "(tenant, " + joiner1 + ") values (#{tenant}, " + joiner2 + ") returning *;";
    return query;
  }

  default Tuple2<String, Map<String, Object>> queryStatementAndParams(Q query, boolean delete) {
    final var queryFilters = new StringJoiner(" and ");
    final var paramMap = optionsParams(query.options());
    queryOptionsFilters(query.options(), queryFilters);
    queryFilters(query, queryFilters, paramMap);
    queryExtraFilters(query, queryFilters);
    final var initialStatement = delete ? "delete from " + table() + " where " : "select * from " + table() + " where ";
    String finalStatement = initialStatement
      + queryFilters
      + getOrder(query.options(), delete)
      + limitAndOffset(query.options(), delete);
    logger.debug("Resulting statement -> " + finalStatement);
    return Tuple2.of(finalStatement, paramMap);
  }

  private void queryFilters(Q query, StringJoiner queryFilters, Map<String, Object> paramMap) {
    queryFieldsColumn(query).forEach(tuple -> addField(queryFilters, paramMap, tuple));

  }

  default void addField(StringJoiner queryFilters, Map<String, Object> paramMap, Tuple2<String, List<?>> tuple) {
    if (tuple.getItem2() != null && !tuple.getItem2().isEmpty()) {
      if (tuple.getItem2().stream().anyMatch(i -> i instanceof Integer)) {
        anyInt(tuple.getItem1(), tuple.getItem2().stream().map(Integer.class::cast).toList(), paramMap, queryFilters);
      }
      if (tuple.getItem2().stream().anyMatch(i -> i instanceof String)) {
        anyString(tuple.getItem1(), tuple.getItem2().stream().map(String.class::cast).toList(), paramMap, queryFilters);
      }
      if (tuple.getItem2().stream().anyMatch(i -> i instanceof Enum)) {
        anyEnum(tuple.getItem1(), tuple.getItem2().stream().map(Enum.class::cast).toList(), paramMap, queryFilters);
      }
      if (tuple.getItem2().stream().anyMatch(i -> i instanceof Long)) {
        anyLong(tuple.getItem1(), tuple.getItem2().stream().map(Long.class::cast).toList(), paramMap, queryFilters);
      }
    }
  }

  default void anyString(String column, List<String> params, Map<String, Object> paramMap, StringJoiner queryString) {
    if (params != null && !params.isEmpty()) {
      final var array = params.stream().map(s -> s.replace("*", "%")).toArray(String[]::new);
      paramMap.put(column, array);
      queryString.add(" " + column + " ILIKE any(#{" + column + "}) ");
    }
  }

  default void anyJsonString(String column, String field, List<String> params, Map<String, Object> paramMap, StringJoiner queryString) {
    if (params != null && !params.isEmpty()) {
      final var array = params.stream().map(s -> s.replace("*", "%")).toArray(String[]::new);
      paramMap.put(column, array);
      queryString.add(" " + column + " ->> '" + field + "' ilike any(#{" + column + "}) ");
    }
  }

  default void anyJsonEnum(String column, String field, List<Enum> params, Map<String, Object> paramMap, StringJoiner queryString) {
    if (params != null && !params.isEmpty()) {
      final var array = params.stream().map(Enum::name).toArray(String[]::new);
      paramMap.put(column, array);
      queryString.add(" " + column + " ->> '" + field + "' ilike any(#{" + column + "}) ");
    }
  }

  default void anyJsonInt(String column, String field, List<Integer> params, Map<String, Object> paramMap, StringJoiner queryString) {
    if (params != null && !params.isEmpty()) {
      final var array = params.toArray(Integer[]::new);
      paramMap.put(column, array);
      queryString.add(" " + column + " ->> '" + field + "' ilike any(#{" + column + "}) ");
    }
  }

  default void anyJsonLong(String column, String field, List<Long> params, Map<String, Object> paramMap, StringJoiner queryString) {
    if (params != null && !params.isEmpty()) {
      final var array = params.toArray(Long[]::new);
      paramMap.put(column, array);
      queryString.add(" " + column + " ->> '" + field + "' ilike any(#{" + column + "}) ");
    }
  }

  default void anyEnum(String column, List<Enum> params, Map<String, Object> paramMap, StringJoiner queryString) {
    if (params != null && !params.isEmpty()) {
      final var array = params.stream().map(Enum::name).toArray(String[]::new);
      paramMap.put(column, array);
      queryString.add(" " + column + " = any(#{" + column + "}) ");
    }
  }

  default void anyInt(String column, List<Integer> params, Map<String, Object> paramMap, StringJoiner queryString) {
    if (params != null && !params.isEmpty()) {
      final var array = params.toArray(Integer[]::new);
      paramMap.put(column, array);
      queryString.add(" " + column + " = any(#{" + column + "}) ");
    }
  }

  default void anyLong(String column, List<Long> params, Map<String, Object> paramMap, StringJoiner queryString) {
    if (params != null && !params.isEmpty()) {
      final var tupleArray = params.toArray(Long[]::new);
      paramMap.put(column, tupleArray);
      queryString.add(" " + column + " = any(#{" + column + "}) ");
    }
  }

  private void queryOptionsFilters(QueryOptions options, StringJoiner queryFilters) {
    if (options.tenant() != null) {
      queryFilters.add(" tenant = #{tenant} ");
    } else {
      queryFilters.add(" tenant like '%' ");
    }
    if (options.creationDateFrom() != null) {
      queryFilters.add(" creation_date >= #{creationDateFrom} ");
    }
    if (options.creationDateTo() != null) {
      queryFilters.add(" creation_date <= #{creationDateTo} ");
    }
    if (options.idFrom() != null) {
      queryFilters.add(" id > #{idFrom} ");
    }
    if (options.lastUpdateFrom() != null) {
      queryFilters.add(" last_update >= #{lastUpdateDateFrom} ");
    }
    if (options.lastUpdateTo() != null) {
      queryFilters.add(" last_update <= #{lastUpdateDateTo} ");
    }
  }

  default Tuple2<String, Map<String, Object>> count(Q query) {
    final var queryFilters = new StringJoiner(" and ");
    final var paramMap = optionsParams(query.options());
    queryOptionsFilters(query.options(), queryFilters);
    queryFilters(query, queryFilters, paramMap);
    queryExtraFilters(query, queryFilters);
    final var initialStatement = "select count(*) as count from " + table() + " where ";
    String finalStatement = initialStatement
      + queryFilters
      + ";";
    logger.debug("Resulting statement -> " + finalStatement);
    return Tuple2.of(finalStatement, paramMap);
  }

  default HashMap<String, Object> optionsParams(QueryOptions options) {
    final var fieldMap = new HashMap<String, Object>();
    if (options.creationDateFrom() != null) {
      fieldMap.put("creationDateFrom", LocalDateTime.ofInstant(options.creationDateFrom(), ZoneOffset.UTC));
    }
    if (options.creationDateTo() != null) {
      fieldMap.put("creationDateTo", LocalDateTime.ofInstant(options.creationDateTo(), ZoneOffset.UTC));
    }
    if (options.lastUpdateFrom() != null) {
      fieldMap.put("lastUpdateFrom", LocalDateTime.ofInstant(options.lastUpdateFrom(), ZoneOffset.UTC));
    }
    if (options.lastUpdateTo() != null) {
      fieldMap.put("lastUpdateTo", LocalDateTime.ofInstant(options.lastUpdateTo(), ZoneOffset.UTC));
    }
    if (options.tenant() != null) {
      fieldMap.put("tenant", options.tenant().generateString());
    }
    if (options.pageSize() != null) {
      fieldMap.put("pageSize", options.pageSize());
    }
    if (options.pageSize() != null && options.pageNumber() != null) {
      fieldMap.put("offSet", options.pageSize() * options.pageNumber());
    }
    if (options.idFrom() != null) {
      fieldMap.put("idFrom", options.idFrom());
    }
    logger.debug("Query Options Params :" + fieldMap);
    return fieldMap;
  }

  default String getOrder(QueryOptions options, boolean delete) {
    if (options.orderBy() != null && !delete) {
      String orderStatement = " order by " + options.orderBy();
      if (Boolean.TRUE.equals(options.desc())) {
        orderStatement = orderStatement + " desc ";
      }
      return orderStatement;
    } else if (!delete) {
      return "order by id";
    } else {
      return "";
    }
  }

  default String limitAndOffset(QueryOptions options, boolean delete) {
    if (options.pageSize() != null && !delete) {
      if (options.pageNumber() != null) {
        return " offset #{offSet} fetch next #{pageSize} rows only;";
      }
      return " fetch first #{pageSize} rows only;";
    }
    if (delete) {
      return " returning id;";
    }
    return ";";
  }

  default String selectByTenant() {
    return "select * from " + table() + " where tenant = #{tenant};";
  }


}
