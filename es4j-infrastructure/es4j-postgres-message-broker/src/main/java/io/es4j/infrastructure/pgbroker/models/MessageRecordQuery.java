package io.es4j.infrastructure.pgbroker.models;


import io.es4j.sql.models.Query;
import io.es4j.sql.models.QueryOptions;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Instant;
import java.util.List;
@RecordBuilder
public record MessageRecordQuery(
  List<String> ids,
  List<MessageState> states,
  Instant scheduledFrom,
  Instant scheduledTo,
  Instant expirationFrom,
  Instant expirationTo,
  Integer priorityFrom,
  Integer priorityTo,
  String partition,
  String partitionKey,
  QueryOptions options
) implements Query {
}
