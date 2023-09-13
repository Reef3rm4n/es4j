package io.es4j.saga;

import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.Map;
import java.util.Set;


@RecordBuilder
public record SagaRecord(
  String id,
  SagaState state,
  Set<SagaTransactionRecord> transactions,
  Map<String, Object> trigger,
  Map<String, Object> payload
) {

  public SagaRecord computeState() {
    if (transactions().stream().allMatch(t -> t.state() == SagaTransactionState.STAGED)) {
      return SagaRecordBuilder.builder(this).state(SagaState.STAGED).build();
    } else if (transactions().stream().allMatch(t -> t.state() == SagaTransactionState.COMMITTED)) {
      return SagaRecordBuilder.builder(this).state(SagaState.COMMITTED).build();
    } else if (transactions().stream().allMatch(t -> t.state() == SagaTransactionState.REVERTED)) {
      return SagaRecordBuilder.builder(this).state(SagaState.REVERTED).build();
    } else if (transactions().stream().anyMatch(t -> t.state().name().contains("FAILURE"))) {
      return SagaRecordBuilder.builder(this).state(SagaState.FAILED).build();
    } else {
      return SagaRecordBuilder.builder(this).state(SagaState.INITIALIZED).build();
    }
  }
}
