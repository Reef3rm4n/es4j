package io.es4j.task;

import io.smallrye.mutiny.Uni;

import java.time.Duration;
import java.util.Optional;


public interface TimerTask {

  Uni<Void> performTask();
  default TimerTaskConfiguration configuration() {
    return new TimerTaskConfiguration(
      LockLevel.LOCAL,
      Duration.ofSeconds(5),
      Duration.ofSeconds(5),
      Duration.ofMinutes(1),
      Optional.empty()
    );
  }


}
