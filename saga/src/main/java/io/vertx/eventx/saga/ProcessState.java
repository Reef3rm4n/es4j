package io.vertx.eventx.saga;

public enum ProcessState {
  FAILED, FAILED_ON_RETRY, PARTIAL_FAILURE, COMPLETED
}
