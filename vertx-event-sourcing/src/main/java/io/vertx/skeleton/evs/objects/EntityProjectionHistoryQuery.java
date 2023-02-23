package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.sql.models.Query;
import io.vertx.skeleton.sql.models.QueryOptions;

import java.util.List;

public record EntityProjectionHistoryQuery(
  List<String> projectionClasses,
  List<String> entityIds,
  QueryOptions options

) implements Query {
}
