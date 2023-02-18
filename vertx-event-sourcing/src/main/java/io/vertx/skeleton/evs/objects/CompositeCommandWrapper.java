package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.models.RequestMetadata;

import java.util.List;

public record CompositeCommandWrapper(
  String entityId,
  List<io.vertx.skeleton.evs.objects.Command> commands,
  RequestMetadata requestMetadata
) implements Command {

}
