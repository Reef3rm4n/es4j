package io.es4j.infrastructure.pgbroker.models;

public enum MessageState {
  CREATED, PROCESSING, SCHEDULED, EXPIRED, RETRY, RETRIES_EXHAUSTED, RECOVERY, PROCESSED, FATAL_FAILURE, PARKED
}
