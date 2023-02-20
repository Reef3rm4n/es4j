package io.vertx.skeleton.sql.models;

import io.vertx.skeleton.sql.models.Query;
import io.vertx.skeleton.sql.models.QueryOptions;

import java.time.Instant;
import java.util.List;

public record TestModelQuery(
  List<String> textFields,
  Instant timestampFieldFrom,
  Instant timestampFieldTo,
  Long longFieldFrom,
  Long longFieldTo,

  Long longEqField,
  List<String> names,
  QueryOptions options
) implements Query {
}
