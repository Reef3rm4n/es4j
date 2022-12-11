package io.vertx.skeleton.evs;

import io.vertx.skeleton.models.RequestMetadata;

public record FakeCommand(
  String entityId,
  RequestMetadata requestMetadata

) implements EntityAggregateCommand {



}
