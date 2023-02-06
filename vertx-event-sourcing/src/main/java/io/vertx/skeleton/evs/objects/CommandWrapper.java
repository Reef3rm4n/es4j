package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.EntityAggregateCommand;
import io.vertx.skeleton.models.RequestMetadata;

public record CommandWrapper(
  String entityId,
  Command command,
  RequestMetadata requestMetadata
) implements EntityAggregateCommand {

}
