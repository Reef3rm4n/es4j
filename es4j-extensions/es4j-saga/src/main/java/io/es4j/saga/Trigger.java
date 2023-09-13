package io.es4j.saga;

import io.smallrye.mutiny.Uni;

public class Trigger {


  public Uni<Void> trigger(Object trigger) {
    // todo if async return
    // todo route request
    return Uni.createFrom().voidItem();
  }
}
