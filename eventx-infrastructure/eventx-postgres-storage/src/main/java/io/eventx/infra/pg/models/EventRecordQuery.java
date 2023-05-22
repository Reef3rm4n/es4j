package io.eventx.infra.pg.models;

import io.eventx.sql.models.Query;
import io.eventx.sql.models.QueryOptions;

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
  QueryOptions options
) implements Query {

}
