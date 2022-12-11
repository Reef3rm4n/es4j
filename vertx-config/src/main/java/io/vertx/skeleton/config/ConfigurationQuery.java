package io.vertx.skeleton.config;

import io.vertx.skeleton.models.Query;
import io.vertx.skeleton.models.QueryOptions;

import java.util.List;

public record ConfigurationQuery(
  List<String> name,
  List<String> tClasses,
  QueryOptions options

) implements Query {


}
