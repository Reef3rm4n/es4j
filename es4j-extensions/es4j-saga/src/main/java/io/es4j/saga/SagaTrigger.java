package io.es4j.saga;

import java.util.UUID;

public interface SagaTrigger {
  default String id() {
    return UUID.randomUUID().toString();
  }

  default boolean fireAndForget() {
    return false;
  }

}
