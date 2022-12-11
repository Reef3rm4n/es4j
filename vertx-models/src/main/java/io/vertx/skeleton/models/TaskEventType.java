package io.vertx.skeleton.models;

public enum TaskEventType {
  PROCESSED,
  FAILED,
  EXPIRED,
  RETRIES_EXHAUSTED,
  RECOVERY_TRIGGERED,
  STUCK
}
