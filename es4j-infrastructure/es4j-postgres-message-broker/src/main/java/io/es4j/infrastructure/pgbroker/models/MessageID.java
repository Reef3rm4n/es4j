package io.es4j.infrastructure.pgbroker.models;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record MessageID(
  String id
) {
}
