package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.EntityAggregateCommand;
import io.vertx.skeleton.models.RequestMetadata;

import java.util.List;

public record CompositeCommandWrapper(
  String entityId,
  List<Command> commands,
  RequestMetadata requestMetadata
) implements EntityAggregateCommand {

}
