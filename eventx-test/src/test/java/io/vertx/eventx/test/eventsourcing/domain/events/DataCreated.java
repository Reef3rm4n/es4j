package io.vertx.eventx.test.eventsourcing.domain.events;



import io.vertx.eventx.Event;

import java.util.Map;

public record DataCreated(
  String entityId,
  Map<String, Object> data
) implements Event {
}
