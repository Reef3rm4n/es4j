package io.es4j.core.objects;


import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;

@RecordBuilder
public record EventJournalFilter(
  String tenant,
  List<String> eventTypes,
  List<String> tags
) {
}
