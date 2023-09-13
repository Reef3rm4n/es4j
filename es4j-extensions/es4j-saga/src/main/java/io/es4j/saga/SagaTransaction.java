package io.es4j.saga;

import io.smallrye.mutiny.Uni;

import java.time.Duration;
import java.util.List;

public interface SagaTransaction<T> {

  String name();

  default Uni<T> stage(T payload) {
    return Uni.createFrom().item(payload);
  }

  Uni<T> commit(T payload);

  default Uni<Void> rollback(T payload, Throwable error) {
    return Uni.createFrom().voidItem();
  }

  default TransactionConfiguration configuration() {
    return new TransactionConfiguration(
      5,
      Duration.ofMillis(10),
      List.of(RuntimeException.class)
    );
  }
}
