package io.es4j.core.objects;


import io.soabase.recordbuilder.core.RecordBuilder;
import io.es4j.Event;

import java.util.List;
import java.util.Map;

@RecordBuilder
public record SnapshotEvent(
  Map<String, Object> state,
  List<String> knownCommands,
  Long currentVersion
) implements Event {

}
