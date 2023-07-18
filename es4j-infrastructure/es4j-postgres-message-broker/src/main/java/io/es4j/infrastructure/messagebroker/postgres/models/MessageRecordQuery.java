package io.es4j.infrastructure.messagebroker.postgres.models;

import io.es4j.sql.models.Query;
import io.es4j.sql.models.QueryOptions;
import io.soabase.recordbuilder.core.RecordBuilder;
import io.es4j.infrastructure.messagebroker.models.MessageState;

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
  Integer retryCounterFrom,
  Integer retryCounterTo,
  QueryOptions options
) implements Query {
}
