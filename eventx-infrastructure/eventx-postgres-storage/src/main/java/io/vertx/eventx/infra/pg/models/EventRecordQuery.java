package io.vertx.eventx.infra.pg.models;

import io.vertx.eventx.sql.models.Query;
import io.vertx.eventx.sql.models.QueryOptions;

import java.util.List;

public record EventRecordQuery(
  List<String> entityId,
  List<String> eventClasses,
  List<String> aggregateClasses,
  List<String> tags,
  Long eventVersionFrom,
  Long eventVersionTo,
  Long idFrom,
  Long idTo,
  List<String> commandClasses,
  QueryOptions options
) implements Query {

}
