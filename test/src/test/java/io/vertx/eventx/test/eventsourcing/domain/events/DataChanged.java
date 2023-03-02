package io.vertx.eventx.test.eventsourcing.domain.events;

import java.util.Map;

public record DataChanged(
  Map<String,Object> newData
) {
}
