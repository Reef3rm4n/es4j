package io.es4j.saga;

public enum SagaTransactionState {
  INITIALIZED,
  STAGED,
  COMMITTED,
  STAGE_FAILURE,
  COMMIT_FAILURE,
  REVERTED,
  REVERT_FAILURE,
}
