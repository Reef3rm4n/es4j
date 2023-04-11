package io.vertx.eventx.queue.postgres.models;


import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.eventx.sql.models.Query;
import io.vertx.eventx.sql.models.QueryOptions;

import java.util.List;

@RecordBuilder
public record MessageTransactionQuery(
  List<String> ids,
  List<String> processors,
  QueryOptions options
) implements Query {
}
