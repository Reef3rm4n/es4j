package io.es4j.sql;

import io.es4j.sql.generator.filters.QueryBuilder;
import io.es4j.sql.misc.Constants;
import io.es4j.sql.models.Query;
import io.es4j.sql.models.RepositoryRecord;
import io.es4j.sql.models.RepositoryRecordKey;
import io.es4j.sql.models.BaseRecord;
import io.vertx.sqlclient.Row;

import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

import static io.es4j.sql.misc.Constants.VERSION;


public interface RecordMapper<K extends RepositoryRecordKey, V extends RepositoryRecord<V>, Q extends Query> {

  /**
   * @return table name
   */
  String table();

  /**
   * Should return a set that contains all the columns that make up the table
   * * base record columns can be skipped
   *
   * @return
   */
  Set<String> columns();

  /**
   * Should return a set that contains all the columns that can be updated during update queries. used for updateByKey(), updateById()
   *
   * @return
   */
  default Set<String> updatableColumns() {
    return columns().stream()
      .filter(c -> keyColumns().stream().noneMatch(k -> k.equalsIgnoreCase(c)))
      .collect(Collectors.toSet());
  }

  /**
   * Should return all the columns that make up the key of a record
   *
   * @return
   */
  Set<String> keyColumns();

  /**
   * Should map a vert.x Row to the domain object V
   *
   * @param row the vertx row that represents the row's from the database
   * @return
   */
  V rowMapper(Row row);


  /**
   * The map should be filled with the tuple(column,valueParam) of a record that will be stored in the database, used for inserts only
   *
   * @param params
   */
  void params(Map<String, Object> params, V actualRecord);

  /**
   * The map should be filled with tuple(column,valueParam) that compose the key of the record, used for selectByKey()
   *
   * @param params
   */
  void keyParams(Map<String, Object> params, K key);

  /**
   * Fill up the builder with entries Column,Value this mapping will be subjected to run time validation thus the values will always be checked before being added to the final query
   *
   * @param query   the object that represents the queryable fields in the record
   * @param builder where queries for that object can be built
   */
  void queryBuilder(Q query, QueryBuilder builder);

  default BaseRecord baseRecord(Row row) {
    return new BaseRecord(
      getTenant(row),
      getVersion(row),
      row.getLocalDateTime(Constants.CREATION_DATE).toInstant(ZoneOffset.UTC),
      row.getLocalDateTime(Constants.LAST_UPDATE).toInstant(ZoneOffset.UTC)
    );
  }

  private static String getTenant(Row row) {
    try {
      return row.getString(Constants.TENANT);
    } catch (Exception e) {
      return "default";
    }
  }

  private static Integer getVersion(Row row) {
    try {
      return row.getInteger(VERSION);
    } catch (Exception e) {
      return 0;
    }

  }


}
