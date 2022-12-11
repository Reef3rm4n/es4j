package io.vertx.skeleton.models;

import java.util.List;

public record EventJournalQuery(
  List<String> entityId,
  List<String> eventType,
  Long eventVersionFrom,
  QueryOptions options
) implements Query {

}
