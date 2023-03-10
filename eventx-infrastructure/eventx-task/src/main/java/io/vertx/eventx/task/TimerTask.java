package io.vertx.eventx.task;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.sql.exceptions.NotFound;

import java.util.List;


public interface TimerTask {

  Uni<Void> performTask();
  default TimerTaskConfiguration configuration() {
    return new TimerTaskConfiguration(
      LockLevel.LOCAL,
      5000L,
      5L,
      10L,
      1L,
      List.of(NotFound.class)
    );
  }


}
