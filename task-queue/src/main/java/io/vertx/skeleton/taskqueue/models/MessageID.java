package io.vertx.skeleton.taskqueue.models;


public record MessageID(
  String id,
  String tenant
) {
}
