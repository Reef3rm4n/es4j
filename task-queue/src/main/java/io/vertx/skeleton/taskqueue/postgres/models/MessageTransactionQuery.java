package io.vertx.skeleton.taskqueue.postgres.models;


import io.vertx.skeleton.sql.models.Query;
import io.vertx.skeleton.sql.models.QueryOptions;

import java.util.List;

public record MessageTransactionQuery(
  List<String> ids,
  List<String> processors,
  QueryOptions options
) implements Query {
}
