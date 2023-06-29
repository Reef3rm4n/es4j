package io.es4j.core.objects;

import io.es4j.Event;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;

@RecordBuilder
public record EventJournalFilter(
  List<Class<? extends Event>> events,
  List<String> tags
) {
}
