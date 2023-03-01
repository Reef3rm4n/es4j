package io.vertx.eventx.config;

import io.vertx.eventx.sql.models.Query;
import io.vertx.eventx.sql.models.QueryOptions;

public record ConfigurationSearch(
  String name,
  String tClass,
  QueryOptions options
) implements Query {
}
