package io.vertx.eventx.objects;

import io.vertx.eventx.Command;

public record LoadAggregate(
  String aggregateId,
  CommandHeaders headers

) implements Command {
}
