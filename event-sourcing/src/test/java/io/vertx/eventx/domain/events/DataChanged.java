package io.vertx.eventx.domain.events;

import java.util.Map;

public record DataChanged(
  Map<String,Object> newData
) {
}
