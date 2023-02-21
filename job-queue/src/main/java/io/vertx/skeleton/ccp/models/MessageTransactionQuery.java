package io.vertx.skeleton.ccp.models;


import io.vertx.skeleton.sql.models.Query;
import io.vertx.skeleton.sql.models.QueryOptions;

import java.util.List;

public record MessageTransactionQuery(
  List<String> ids,
  List<String> processors,
  List<ProcessorType> types,
  QueryOptions options
) implements Query {
}
