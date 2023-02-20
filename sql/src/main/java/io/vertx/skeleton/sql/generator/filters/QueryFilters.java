package io.vertx.skeleton.sql.generator.filters;

import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class QueryFilters {
  public final List<Tuple2<String, List<?>>> eqFilters;
  public final List<Tuple2<String, List<?>>> likeFilters;
  public final List<Tuple2<String, List<?>>> iLikeFilters;
  public final List<Tuple2<String, ?>> fromRangeFilters;
  public final List<Tuple2<String, ?>> toRangeFilters;
  public final List<Tuple3<String, Queue<String>, List<?>>> jsonEqFilter;
  public final List<Tuple3<String, Queue<String>, List<?>>> jsonLikeFilters;
  public final List<Tuple3<String, Queue<String>, List<?>>> jsonILikeFilters;
  public final List<Tuple3<String, Queue<String>, ?>> jsonFromFilters;
  public final List<Tuple3<String, Queue<String>, ?>> jsonToFilters;
  public final List<String> order;
  public final Boolean deleteQuery;

  public QueryFilters() {
    // array list can cause overhead but should be fine for most use cases, one improvement would be to use straight arrays instead.
    this.eqFilters = new ArrayList<>();
    this.likeFilters = new ArrayList<>();
    this.iLikeFilters = new ArrayList<>();
    this.fromRangeFilters = new ArrayList<>();
    this.toRangeFilters = new ArrayList<>();
    this.jsonEqFilter = new ArrayList<>();
    this.jsonLikeFilters = new ArrayList<>();
    this.jsonILikeFilters = new ArrayList<>();
    this.order = new ArrayList<>();
    this.jsonFromFilters = new ArrayList<>();
    this.jsonToFilters = new ArrayList<>();
    this.deleteQuery = false;
  }

  public QueryFilters(Boolean deleteQuery) {
    // array list can cause overhead but should be fine for most use cases, one improvement would be to use straight arrays instead.
    this.eqFilters = new ArrayList<>();
    this.likeFilters = new ArrayList<>();
    this.iLikeFilters = new ArrayList<>();
    this.fromRangeFilters = new ArrayList<>();
    this.toRangeFilters = new ArrayList<>();
    this.jsonEqFilter = new ArrayList<>();
    this.jsonLikeFilters = new ArrayList<>();
    this.jsonILikeFilters = new ArrayList<>();
    this.order = new ArrayList<>();
    this.jsonFromFilters = new ArrayList<>();
    this.jsonToFilters = new ArrayList<>();
    this.deleteQuery = deleteQuery;
  }
}
