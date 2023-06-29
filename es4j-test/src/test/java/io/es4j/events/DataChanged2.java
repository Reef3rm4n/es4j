package io.es4j.events;

import io.es4j.Event;

import java.util.Map;

public record DataChanged2(
  Map<String,Object> newData
) implements Event {
}
