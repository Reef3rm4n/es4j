package io.vertx.eventx.queue.models;


import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record MessageID(
  String id,
  String tenant
) {
}
