package io.eventx.events;



import io.eventx.Event;

import java.util.Map;

public record DataCreated(
  String entityId,
  Map<String, Object> data
) implements Event {
}
