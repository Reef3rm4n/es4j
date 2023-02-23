package io.vertx.skeleton.evs.objects;


public record PolledEvent(
  String entityId,
  String tenant,
  Object event
) {
}
