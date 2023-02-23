package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.models.RequestHeaders;
import io.vertx.skeleton.models.RequestMetadata;

public record CommandWrapper(
  String entityId,
  io.vertx.skeleton.evs.objects.Command command,
  RequestHeaders requestHeaders
) implements Command {

}
