package io.vertx.skeleton.sql.generator.filters;

import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;

import java.util.Arrays;
import java.util.List;

public class QueryBuilder {
    private final QueryFilters filters;

    public QueryBuilder() {
        this.filters = new QueryFilters();
    }

    public QueryBuilder(boolean delete) {
        this.filters = new QueryFilters(delete);
    }

    public QueryBuilder eq(String column, Object... values) {
        filters.eqFilters.add(Tuple2.of(column, List.of(values)));
        return this;
    }

    public QueryBuilder like(String column, Object... values) {
        filters.likeFilters.add(Tuple2.of(column, List.of(values)));
        return this;
    }

    public QueryBuilder iLike(String column, Object... values) {
        filters.iLikeFilters.add(Tuple2.of(column, List.of(values)));
        return this;
    }

    public QueryBuilder from(String column, Object value) {
        filters.fromRangeFilters.add(Tuple2.of(column, value));
        return this;
    }

    public QueryBuilder to(String column, Object value) {
        filters.toRangeFilters.add(Tuple2.of(column, value));
        return this;
    }

    public QueryBuilder jsonEq(String column,String jsonField, Object... values) {
        filters.jsonEqFilter.add(Tuple3.of(column, jsonField,List.of(values)));
        return this;
    }

    public QueryBuilder jsonLike(String column, String jsonField, Object... values) {
        filters.jsonLikeFilters.add(Tuple3.of(column, jsonField, List.of(values)));
        return this;
    }

    public QueryBuilder jsonILike(String column, String jsonField, Object... values) {
        filters.jsonILikeFilters.add(Tuple3.of(column, jsonField, List.of(values)));
        return this;
    }


    public QueryBuilder orderBy(String column) {
        filters.order.add(column);
        return this;
    }

    public Boolean isEmpty() {
        return checkFilters(
           filters. eqFilters,
           filters. likeFilters,
           filters. iLikeFilters,
           filters. fromRangeFilters,
           filters. toRangeFilters,
           filters. jsonEqFilter,
           filters. jsonLikeFilters,
           filters.jsonILikeFilters
        );
    }

    private Boolean checkFilters(List<?>... lists) {
       return Arrays.stream(lists).anyMatch(List::isEmpty);
    }

    public QueryFilters filters() {
        return filters;
    }


}
