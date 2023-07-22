package io.es4j.infrastructure.pgbroker.models;


import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;
import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;

import java.time.Instant;

@RecordBuilder
public record DeadLetterRecord(
  String id,
  Instant scheduled,
  Instant expiration,
  Integer priority,
  Integer retryCounter,
  MessageState messageState,
  String payloadClass,
  JsonObject payload,
  JsonObject failedProcessors,
  String verticleId,
  String partitionId,
  String partitionKey,
  BaseRecord baseRecord
) implements RepositoryRecord<DeadLetterRecord> {

  @Override
  public DeadLetterRecord with(BaseRecord baseRecord) {
    return DeadLetterRecordBuilder.builder(this)
      .baseRecord(baseRecord)
      .build();
  }
}
