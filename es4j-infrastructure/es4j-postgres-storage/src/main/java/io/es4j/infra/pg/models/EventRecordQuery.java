package io.es4j.infra.pg.models;

import io.es4j.sql.models.Query;
import io.es4j.sql.models.QueryOptions;

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
