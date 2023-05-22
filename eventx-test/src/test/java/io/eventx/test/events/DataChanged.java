package io.eventx.test.events;

import io.eventx.Event;

import java.util.Map;

public record DataChanged(
  Map<String,Object> newData
) implements Event {
}
