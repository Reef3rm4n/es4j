package io.eventx.infra.pg.models;

import io.eventx.sql.models.Query;
import io.eventx.sql.models.QueryOptions;

import java.util.List;

public record ProjectionHistoryQuery(
  List<String> projectionClasses,
  List<String> entityIds,
  QueryOptions options

) implements Query {
}
