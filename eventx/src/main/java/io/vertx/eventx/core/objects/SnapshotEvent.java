package io.vertx.eventx.core.objects;


import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.eventx.Event;

import java.util.List;
import java.util.Map;

@RecordBuilder
public record SnapshotEvent(
  Map<String, Object> state,
  List<String> knownCommands,
  Long currentVersion
) implements Event {

}
