package io.eventx.core.objects;

import io.eventx.Event;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;

@RecordBuilder
public record EventJournalFilter(
  List<Class<? extends Event>> events,
  List<String> tags
) {
}
