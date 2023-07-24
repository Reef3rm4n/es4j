package io.es4j.infrastructure.pgbroker.models;

public enum MessageState {
  PUBLISHED, CONSUMING, EXPIRED, STUCK, CONSUMED
}
