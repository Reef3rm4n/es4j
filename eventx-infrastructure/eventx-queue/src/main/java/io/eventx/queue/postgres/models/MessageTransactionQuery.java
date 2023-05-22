package io.eventx.queue.postgres.models;


import io.eventx.sql.models.Query;
import io.eventx.sql.models.QueryOptions;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;

@RecordBuilder
public record MessageTransactionQuery(
  List<String> ids,
  List<String> processors,
  QueryOptions options
) implements Query {
}
