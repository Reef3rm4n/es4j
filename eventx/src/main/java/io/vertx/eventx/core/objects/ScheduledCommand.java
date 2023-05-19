package io.vertx.eventx.core.objects;


import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.Map;

@RecordBuilder
public record ScheduledCommand(
  Map<String, Object> command
) {
}
