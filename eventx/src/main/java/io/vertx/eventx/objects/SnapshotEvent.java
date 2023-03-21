package io.vertx.eventx.objects;


import io.vertx.eventx.Event;

import java.util.List;
import java.util.Map;

public record SnapshotEvent(
  Map<String, Object> state,
  List<String> knownCommands,
  Long currentVersion
) implements Event {

}
