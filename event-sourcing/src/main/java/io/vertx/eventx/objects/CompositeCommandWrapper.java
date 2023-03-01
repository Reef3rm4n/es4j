package io.vertx.eventx.objects;

import io.vertx.eventx.Command;
import io.vertx.eventx.common.CommandHeaders;

import java.util.List;

public record CompositeCommandWrapper(
  String entityId,
  List<io.vertx.eventx.objects.Command> commands,
  CommandHeaders headers
) implements Command {

}
