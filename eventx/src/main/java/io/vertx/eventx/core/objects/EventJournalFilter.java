package io.vertx.eventx.core.objects;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Event;

import java.util.List;

@RecordBuilder
public record EventJournalFilter(
  List<Class<? extends Event>> events,
  List<String> tags
) {
}
