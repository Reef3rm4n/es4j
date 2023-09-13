package io.es4j.saga;

public enum SagaState {
  INITIALIZED, STAGED, COMMITTED, REVERTED, FAILED
}
