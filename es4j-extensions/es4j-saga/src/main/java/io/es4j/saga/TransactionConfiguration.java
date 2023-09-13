package io.es4j.saga;

import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Duration;
import java.util.List;

@RecordBuilder
public record TransactionConfiguration(
  Integer numberOfRetries,
  Duration retryBackOff,
  List<Class<? extends Throwable>> retryableFailures
) {
}
