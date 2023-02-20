package io.vertx.skeleton.sql.generator.filters;

import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.skeleton.sql.models.JsonQueryParam;
import io.vertx.skeleton.sql.models.JsonQueryParams;
import io.vertx.skeleton.sql.models.QueryParam;
import io.vertx.skeleton.sql.models.QueryParams;

import java.util.*;

public class QueryBuilder {
  private final QueryFilters filters;

  public QueryBuilder() {
    this.filters = new QueryFilters();
  }

  public QueryBuilder(boolean delete) {
    this.filters = new QueryFilters(delete);
  }

  public <T> QueryBuilder eq(QueryParams<T> queryParams) {
    filters.eqFilters.add(Tuple2.of(queryParams.column(), queryParams.params()));
    return this;
  }

  public <T> QueryBuilder like(QueryParams<T> queryParams) {
    filters.likeFilters.add(Tuple2.of(queryParams.column(), queryParams.params()));
    return this;
  }

  public <T> QueryBuilder iLike(QueryParams<T> queryParams) {
    filters.iLikeFilters.add(Tuple2.of(queryParams.column(), unpackValues(queryParams.params().toArray())));
    return this;
  }


  private <T> void validateQueryParam(QueryParam<T> queryParams) {
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

  public <T> QueryBuilder from(QueryParam<T> queryParam) {
    filters.fromRangeFilters.add(Tuple2.of(queryParam.column(), queryParam.param()));
    return this;
  }

  public <T> QueryBuilder to(QueryParam<T> queryParam) {
    filters.toRangeFilters.add(Tuple2.of(queryParam.column(), queryParam.param()));
    return this;
  }

  public <T> QueryBuilder jsonEq(JsonQueryParams<T> jsonQueryParams) {
    filters.jsonEqFilter.add(Tuple3.of(jsonQueryParams.column(), jsonQueryParams.jsonFields(), jsonQueryParams.params()));
    return this;
  }

  public <T> QueryBuilder jsonLike(JsonQueryParams<T> jsonQueryParams) {
    filters.jsonLikeFilters.add(Tuple3.of(jsonQueryParams.column(), jsonQueryParams.jsonFields(), jsonQueryParams.params()));
    return this;
  }

  public <T> QueryBuilder jsonILike(JsonQueryParams<T> jsonQueryParams) {
    filters.jsonILikeFilters.add(Tuple3.of(jsonQueryParams.column(), jsonQueryParams.jsonFields(), jsonQueryParams.params()));
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
