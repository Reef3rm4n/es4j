package io.es4j.task;

import io.es4j.sql.exceptions.NotFound;
import io.smallrye.mutiny.Uni;

import java.time.Duration;
import java.util.List;


public interface TimerTask {

  Uni<Void> performTask();
  default TimerTaskConfiguration configuration() {
    return new TimerTaskConfiguration(
      LockLevel.LOCAL,
      Duration.ofSeconds(5),
      Duration.ofSeconds(5),
      Duration.ofMinutes(1),
      Duration.ofMinutes(1),
      List.of(NotFound.class)
    );
  }


}
