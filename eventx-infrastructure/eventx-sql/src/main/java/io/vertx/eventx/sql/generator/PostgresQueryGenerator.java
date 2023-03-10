package io.vertx.eventx.sql.generator;

import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.eventx.sql.QueryGenerator;
import io.vertx.eventx.sql.commands.*;
import io.vertx.eventx.sql.generator.filters.*;
import io.vertx.eventx.sql.models.QueryOptions;

import java.util.Map;
import java.util.StringJoiner;

public class PostgresQueryGenerator implements QueryGenerator {

    public static final PostgresQueryGenerator INSTANCE = new PostgresQueryGenerator();

    private PostgresQueryGenerator() {
    }

    @Override
    public String updateByKey(GenerateUpdateByKeyStatement generateUpdateByKeyStatement) {
        final var keyJoiner = new StringJoiner(" and ");
        generateUpdateByKeyStatement.keyColumns().forEach(c -> keyJoiner.add(c + " = #{" + c + "}"));
        final var paramsJoiner = new StringJoiner(", ");
        generateUpdateByKeyStatement.updateAbleColumns().forEach(c -> paramsJoiner.add(c + " = #{" + c + "}"));
        return "update " + generateUpdateByKeyStatement.table() + " set updated = current_timestamp, rec_version = rec_version + 1, " + paramsJoiner + " where " + keyJoiner + " returning *;";
    }

    @Override
    public String insert(GenerateInsertStatement generateInsertStatement) {
        final var columnsJoiner = new StringJoiner(", ");
        final var paramsJoiner = new StringJoiner(", ");
        generateInsertStatement.columns().forEach(columnsJoiner::add);
        generateInsertStatement.columns().forEach(c -> paramsJoiner.add("#{" + c + "}"));
        return "insert into " + generateInsertStatement.table() + "(tenantId, " + columnsJoiner + ") values (#{tenantId}, " + paramsJoiner + ") returning *;";
    }

    @Override
    public String selectByKey(GenerateSelectByKeyStatement generateSelectByKeyStatement) {
        final var selectByKeyJoiner = new StringJoiner(" and ");
        generateSelectByKeyStatement.keyColumns().forEach(c -> selectByKeyJoiner.add(c + " = #{" + c + "}"));
        return "select * from " + generateSelectByKeyStatement.table() + " where " + selectByKeyJoiner + " ;";
    }

    @Override
    public String selectAll(String table) {
        return "select * from " + table + ";";
    }

    @Override
    public String deleteByKey(GenerateDeleteByKeyStatement generateDeleteByKeyStatement) {
        final var deleteJoiner = new StringJoiner(" and ");
        generateDeleteByKeyStatement.keyColumns().forEach(c -> deleteJoiner.add(c + " = #{" + c + "}"));
        return "delete from " + generateDeleteByKeyStatement.table() + " where " + deleteJoiner + " returning *;";
    }

    @Override
    public Tuple2<String, Map<String, Object>> query(GenerateQueryStatement generateQueryStatement) {
        return switch (generateQueryStatement.type()) {
            case COUNT -> count(generateQueryStatement.table(), generateQueryStatement.queryBuilder(),generateQueryStatement.queryOptions());
            case DELETE -> deleteQuery(generateQueryStatement);
            case SELECT -> selectQuery(generateQueryStatement);
        };
    }

  private Tuple2<String, Map<String, Object>> selectQuery(GenerateQueryStatement generateQueryStatement) {
    final var queryJoiner = new StringJoiner(" and ");
    final var paramMap = OptionsFilter.optionsParams(generateQueryStatement.queryOptions());
    OptionsFilter.addOptionsQueryFiltersAndParams(generateQueryStatement.queryOptions(), queryJoiner);
    addQueryFiltersAndParam(queryJoiner, generateQueryStatement.queryBuilder(), paramMap);
    final var initialStatement = "select * from " + generateQueryStatement.table() + " where ";
    final var finalStatement = initialStatement
      + queryJoiner
      + OptionsFilter.getOrder(generateQueryStatement.queryOptions(), generateQueryStatement.queryBuilder().deleteQuery)
      + OptionsFilter.limitAndOffset(generateQueryStatement.queryOptions(), generateQueryStatement.queryBuilder().deleteQuery);
    return Tuple2.of(finalStatement, paramMap);
  }

  public Tuple2<String, Map<String, Object>> deleteQuery(GenerateQueryStatement generateQueryStatement) {
        final var queryJoiner = new StringJoiner(" and ");
        final var paramMap = OptionsFilter.optionsParams(generateQueryStatement.queryOptions());
        OptionsFilter.addOptionsQueryFiltersAndParams(generateQueryStatement.queryOptions(), queryJoiner);
        addQueryFiltersAndParam(queryJoiner, generateQueryStatement.queryBuilder(), paramMap);
        final var initialStatement = "delete from " + generateQueryStatement.table() + " where ";
        String finalStatement = initialStatement
            + queryJoiner
            + OptionsFilter.getOrder(generateQueryStatement.queryOptions(), true)
            + OptionsFilter.limitAndOffset(generateQueryStatement.queryOptions(), true);
        return Tuple2.of(finalStatement, paramMap);
    }



    public Tuple2<String, Map<String, Object>> count(String table, QueryFilters queryBuilder, QueryOptions queryOptions) {
        final var queryJoiner = new StringJoiner(" and ");
        final var paramMap = OptionsFilter.optionsParams(queryOptions);
        OptionsFilter.addOptionsQueryFiltersAndParams(queryOptions, queryJoiner);
        addQueryFiltersAndParam(queryJoiner, queryBuilder, paramMap);
        String finalStatement = "select count(*) as count from " + table + " where " + queryJoiner + ";";
        return Tuple2.of(finalStatement, paramMap);
    }

    private void addQueryFiltersAndParam(StringJoiner queryJoiner, QueryFilters filters, Map<String, Object> paramMap) {
        filters.iLikeFilters.forEach(filter -> SimpleFilter.addIlikeFilter(queryJoiner, paramMap, filter));
        filters.likeFilters.forEach(filter -> SimpleFilter.addLikeFilter(queryJoiner, paramMap, filter));
        filters.eqFilters.forEach(filter -> SimpleFilter.addEqFilter(queryJoiner, paramMap, filter));
        filters.fromRangeFilters.forEach(filter -> RangeFilter.addFromFilter(queryJoiner, filter.getItem1(), filter.getItem2()));
        filters.toRangeFilters.forEach(filter -> RangeFilter.addToFilter(queryJoiner, filter.getItem1(), filter.getItem2()));
        filters.jsonEqFilter.forEach(filter -> JsonFilter.addFieldJson(queryJoiner, paramMap, filter));
        filters.jsonILikeFilters.forEach(filter -> JsonFilter.addFieldJson(queryJoiner, paramMap, filter));
        filters.jsonLikeFilters.forEach(filter -> JsonFilter.addFieldJson(queryJoiner, paramMap, filter));
    }
}
