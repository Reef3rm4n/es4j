package io.vertx.skeleton.sql;

import io.vertx.skeleton.sql.generator.filters.QueryBuilder;
import io.vertx.skeleton.sql.models.Query;
import io.vertx.skeleton.sql.models.RecordWithoutID;
import io.vertx.skeleton.sql.models.RepositoryRecord;
import io.vertx.skeleton.sql.models.RepositoryRecordKey;
import io.vertx.sqlclient.Row;

import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

import static io.vertx.skeleton.sql.misc.Constants.*;


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
    void params(Map<String, Object> params, V record);

    /**
     * The map should be filled with tuple(column,valueParam) that compose the key of the record, used for selectByKey()
     *
     * @param params
     */
    void keyParams(Map<String, Object> params, K key);

    /**
     * Fill up the builder with entries <Column,Value> this mapping will be subjected to run time validation thus the values will always be checked before being added to the final query
     *
     * @param query   the object that represents the queryable fields in the record
     * @param builder where queries for that object can be built
     */
    void queryBuilder(Q query, QueryBuilder builder);

    default RecordWithoutID baseRecord(Row row) {
        return new RecordWithoutID(
            row.getString(TENANT),
            row.getInteger(VERSION),
            row.getLocalDateTime(CREATION_DATE).toInstant(ZoneOffset.UTC),
            row.getLocalDateTime(LAST_UPDATE).toInstant(ZoneOffset.UTC)
        );
    }


}
