package io.vertx.eventx.test.domain.events;



import java.util.Map;

public record DataCreated(
  String entityId,
  Map<String, Object> data
) {
}
