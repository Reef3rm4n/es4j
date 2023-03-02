package io.vertx.eventx.test.domain.events;

import java.util.Map;

public record DataChanged(
  Map<String,Object> newData
) {
}
