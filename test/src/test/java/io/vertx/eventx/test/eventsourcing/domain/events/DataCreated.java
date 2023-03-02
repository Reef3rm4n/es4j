package io.vertx.eventx.test.eventsourcing.domain.events;



import java.util.Map;

public record DataCreated(
  String entityId,
  Map<String, Object> data
) {
}
