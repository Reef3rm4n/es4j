package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.sql.models.Query;
import io.vertx.skeleton.sql.models.QueryOptions;

import java.util.List;

public record EventJournalQuery(
  List<String> entityId,
  List<String> eventClasses,
  Long eventVersionFrom,
  Long eventVersionTo,
  Long idFrom,
  Long idTo,
  List<String> commandClasses,
  QueryOptions options
) implements Query {

}
