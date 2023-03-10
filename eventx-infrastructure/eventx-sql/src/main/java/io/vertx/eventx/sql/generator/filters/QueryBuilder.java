package io.vertx.eventx.sql.generator.filters;

import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.eventx.sql.models.JsonQueryParam;
import io.vertx.eventx.sql.models.JsonQueryFilter;
import io.vertx.eventx.sql.models.QueryFilter;
import io.vertx.eventx.sql.models.QueryFilters;

import java.util.*;

public class QueryBuilder {
  private final io.vertx.eventx.sql.generator.filters.QueryFilters filters;

  public QueryBuilder() {
    this.filters = new io.vertx.eventx.sql.generator.filters.QueryFilters();
  }

  public QueryBuilder(boolean delete) {
    this.filters = new io.vertx.eventx.sql.generator.filters.QueryFilters(delete);
  }

  public <T> QueryBuilder eq(QueryFilters<T> queryFilters) {
    filters.eqFilters.add(Tuple2.of(queryFilters.column(), queryFilters.params()));
    return this;
  }

  public <T> QueryBuilder like(QueryFilters<T> queryFilters) {
    filters.likeFilters.add(Tuple2.of(queryFilters.column(), queryFilters.params()));
    return this;
  }

  public <T> QueryBuilder iLike(QueryFilters<T> queryFilters) {
    filters.iLikeFilters.add(Tuple2.of(queryFilters.column(), queryFilters.params()));
    return this;
  }


  private <T> void validateQueryParam(QueryFilter<T> queryParams) {
    Objects.requireNonNull(queryParams.column(),"Column shouldn't be null !");
  }

  public QueryBuilder iLike(String column, Object... values) {
    filters.iLikeFilters.add(Tuple2.of(column, unpackValues(values)));
    return this;
  }

  private static List<?> unpackValues(Object[] values) {
    return Arrays.stream(values).map(
        value -> {
          if (value instanceof Collection<?> collection) {
            return collection.stream().toList();
          } else return List.of(value);
        }
      )
      .flatMap(List::stream)
      .toList();
  }

  public <T> QueryBuilder from(QueryFilter<T> queryFilter) {
    filters.fromRangeFilters.add(Tuple2.of(queryFilter.column(), queryFilter.param()));
    return this;
  }

  public <T> QueryBuilder to(QueryFilter<T> queryFilter) {
    filters.toRangeFilters.add(Tuple2.of(queryFilter.column(), queryFilter.param()));
    return this;
  }

  public <T> QueryBuilder jsonEq(JsonQueryFilter<T> jsonQueryFilter) {
    filters.jsonEqFilter.add(Tuple3.of(jsonQueryFilter.column(), jsonQueryFilter.jsonFields(), jsonQueryFilter.params()));
    return this;
  }

  public <T> QueryBuilder jsonLike(JsonQueryFilter<T> jsonQueryFilter) {
    filters.jsonLikeFilters.add(Tuple3.of(jsonQueryFilter.column(), jsonQueryFilter.jsonFields(), jsonQueryFilter.params()));
    return this;
  }

  public <T> QueryBuilder jsonILike(JsonQueryFilter<T> jsonQueryFilter) {
    filters.jsonILikeFilters.add(Tuple3.of(jsonQueryFilter.column(), jsonQueryFilter.jsonFields(), jsonQueryFilter.params()));
    return this;
  }

  public <T> QueryBuilder jsonFrom(JsonQueryParam<T> jsonQueryParams) {
    filters.jsonFromFilters.add(Tuple3.of(jsonQueryParams.column(), jsonQueryParams.jsonFields(), jsonQueryParams.param()));
    return this;
  }

  public <T> QueryBuilder jsonTo(JsonQueryParam<T> jsonQueryParams) {
    filters.jsonToFilters.add(Tuple3.of(jsonQueryParams.column(), jsonQueryParams.jsonFields(), jsonQueryParams.param()));
    return this;
  }


  public QueryBuilder orderBy(String column) {
    filters.order.add(column);
    return this;
  }

  public Boolean isEmpty() {
    return checkFilters(
      filters.eqFilters,
      filters.likeFilters,
      filters.iLikeFilters,
      filters.fromRangeFilters,
      filters.toRangeFilters,
      filters.jsonEqFilter,
      filters.jsonLikeFilters,
      filters.jsonILikeFilters
    );
  }

  private Boolean checkFilters(List<?>... lists) {
    return Arrays.stream(lists).anyMatch(List::isEmpty);
  }

  public io.vertx.eventx.sql.generator.filters.QueryFilters filters() {
    return filters;
  }


}
