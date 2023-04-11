package io.vertx.eventx.objects;


import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.Map;

@RecordBuilder
public record ScheduledCommand(
  Map<String, Object> command
) {
}
