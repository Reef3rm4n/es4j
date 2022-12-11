package io.vertx.skeleton.config;

import io.vertx.skeleton.models.Query;
import io.vertx.skeleton.models.QueryOptions;

public record ConfigurationSearch(
  String name,
  String tClass,
  QueryOptions options
) implements Query {
}
