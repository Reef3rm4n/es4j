package io.vertx.skeleton.task;

import io.smallrye.mutiny.Uni;


public interface SynchronizedTask {

  Uni<Void> performTask();

  default SynchronizationStrategy strategy() {
    return SynchronizationStrategy.CLUSTER_WIDE;
  }

}
