package io.es4j.queue.models;


import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record MessageID(
  String id,
  String tenant
) {
}
