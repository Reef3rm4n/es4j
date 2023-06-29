package io.es4j.events;



import io.es4j.Event;

import java.util.Map;

public record DataCreated(
  String entityId,
  Map<String, Object> data
) implements Event {
}
