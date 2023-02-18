package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.models.Query;
import io.vertx.skeleton.models.QueryOptions;

import java.util.List;

public record EventJournalQuery(
  List<String> entityId,
  List<String> eventType,
  Long eventVersionFrom,
  QueryOptions options
) implements Query {

}
