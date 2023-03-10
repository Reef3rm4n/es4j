package io.vertx.eventx.objects;

import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Event;

import java.util.List;

public record EventJournalFilter(
  List<Class<? extends Aggregate>> aggregates,
  List<Class<? extends Event>> events,
  List<String> tags
) {
}
