package io.eventx.events;

import io.eventx.Event;

import java.util.Map;

public record DataChanged3(
  Map<String,Object> newData
) implements Event {
}
