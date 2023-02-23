package io.vertx.skeleton.evs;

import io.vertx.skeleton.models.RequestHeaders;

public record FakeCommand(
  String entityId,
  RequestHeaders requestHeaders

) implements Command {



}
