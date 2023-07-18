package io.es4j.infrastructure.messagebroker.models;

public enum MessageState {
  CREATED, PROCESSING, SCHEDULED, EXPIRED, RETRY, RETRIES_EXHAUSTED, RECOVERY, PROCESSED, FATAL_FAILURE
}
