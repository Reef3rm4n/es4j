package io.vertx.eventx.test.eventsourcing.domain.events;

import io.vertx.eventx.Event;

import java.util.Map;

public record DataChanged(
  Map<String,Object> newData
) implements Event {
}
