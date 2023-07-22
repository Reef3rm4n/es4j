package io.es4j.core.objects;


import java.time.Duration;

public record AggregateConfiguration(
  Duration cacheTtl,
  Integer snapshotThreshold,
  Integer commandIdempotencyThreshold
) {

}
