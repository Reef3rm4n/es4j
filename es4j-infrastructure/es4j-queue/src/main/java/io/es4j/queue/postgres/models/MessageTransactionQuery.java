package io.es4j.queue.postgres.models;


import io.es4j.sql.models.Query;
import io.es4j.sql.models.QueryOptions;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;

@RecordBuilder
public record MessageTransactionQuery(
  List<String> ids,
  List<String> processors,
  QueryOptions options
) implements Query {
}
