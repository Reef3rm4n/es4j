package io.es4j.saga;

import java.util.List;

public interface Saga<T, R> {

  List<Class<? extends SagaTransaction<T>>> transactionOrder();

  T supplyPayload(R request);
  default boolean async() {
    return false;
  }

}
