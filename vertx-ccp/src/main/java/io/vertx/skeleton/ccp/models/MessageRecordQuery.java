package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.models.Query;
import io.vertx.skeleton.models.QueryOptions;
import io.vertx.skeleton.models.MessageState;

import java.time.Instant;
import java.util.List;

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
