package io.es4j.infrastructure.messagebroker.models;


import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record MessageID(
  String id,
  String tenant
) {
}
