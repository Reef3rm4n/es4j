package io.es4j.sql.generator.filters;

import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.es4j.sql.models.JsonQueryParam;
import io.es4j.sql.models.JsonQueryFilter;
import io.es4j.sql.models.QueryFilter;


import java.util.*;

public class QueryBuilder {
  private final QueryFilters filters;

  public QueryBuilder() {
    this.filters = new QueryFilters();
  }

  public QueryBuilder(boolean delete) {
    this.filters = new QueryFilters(delete);
  }

  public <T> QueryBuilder eq(io.es4j.sql.models.QueryFilters<T> queryFilters) {
    filters.eqFilters.add(Tuple2.of(queryFilters.column(), queryFilters.params()));
    return this;
  }

  public <T> QueryBuilder like(io.es4j.sql.models.QueryFilters<T> queryFilters) {
    filters.likeFilters.add(Tuple2.of(queryFilters.column(), queryFilters.params()));
    return this;
  }

  public <T> QueryBuilder iLike(io.es4j.sql.models.QueryFilters<T> queryFilters) {
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

  public QueryFilters filters() {
    return filters;
  }


}
