package io.es4j.saga;

import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.Map;
import java.util.Objects;

@RecordBuilder
public record SagaTransactionRecord(
  String transactionName,
  SagaTransactionState state,
  Map<String,Object> failure
) {
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SagaTransactionRecord that = (SagaTransactionRecord) o;
    return Objects.equals(transactionName, that.transactionName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionName);
  }
}
