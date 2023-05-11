package io.vertx.eventx.task;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.sql.exceptions.NotFound;

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
