package io.vertx.eventx.task;

import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.NoStackTraceThrowable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.shareddata.Lock;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


public class TimerTaskDeployer {

  protected static final Logger LOGGER = LoggerFactory.getLogger(TimerTaskDeployer.class);

  public static final Map<Class<?>, Long> timers = new HashMap<>();
  private final Vertx vertx;

  public TimerTaskDeployer(
    Vertx vertx
  ) {
    this.vertx = vertx;
  }

  public void close() {
    timers.forEach((tClass, timerId) -> vertx.cancelTimer(timerId));
  }
  public void deploy(TimerTask timerTask) {
    final var wrapper = new TaskWrapper(timerTask, LoggerFactory.getLogger(timerTask.getClass()));
    triggerTask(wrapper,vertx,10L);
  }

  public static void triggerTask(TaskWrapper taskWrapper, Vertx vertx, Long throttle) {
    timers.remove(taskWrapper.task().getClass());
    final var timerId = vertx.setTimer(
      throttle,
      delay -> {
        final var start = Instant.now();
        final var lockUni = switch (taskWrapper.task().configuration().lockLevel()) {
          case CLUSTER_WIDE -> vertx.sharedData().getLock(taskWrapper.task().getClass().getName());
          case LOCAL -> vertx.sharedData().getLocalLock(taskWrapper.task().getClass().getName());
          case NONE -> Uni.createFrom().item(Lock.newInstance(() -> {
          }));
        };
        lockUni.flatMap(lock -> taskWrapper.task().performTask().onItemOrFailure().invoke((avoid, failure) -> lock.release()))
          .subscribe()
          .with(avoid -> {
              final var end = Instant.now();
              final var emptyTaskBackOff = taskWrapper.task().configuration().throttleInMs();
              taskWrapper.logger().info("Task ran in " + Duration.between(start, end).toMillis() + "ms. Throttling for " + emptyTaskBackOff + "ms");
              triggerTask(taskWrapper, vertx, emptyTaskBackOff);
            },
            throwable -> {
              final var end = Instant.now();
              if (taskWrapper.task.configuration().knownInterruptions().stream().anyMatch(t -> t.isAssignableFrom(throwable.getClass()))) {
                taskWrapper.logger().debug("Task interrupted by" + throwable.getClass().getSimpleName() + " after " + Duration.between(start, end).toMillis() + "ms");
                final var interruptionBackOff = taskWrapper.task().configuration().interruptionBackOff();
                taskWrapper.logger().info("Interrupted, backing off " + interruptionBackOff + "ms");
                triggerTask(taskWrapper, vertx, interruptionBackOff);
              } else if (throwable instanceof NoStackTraceThrowable noStackTraceThrowable && noStackTraceThrowable.getMessage().contains("Timed out waiting to get lock")) {
                final var lockBackOff = taskWrapper.task().configuration().lockBackOffInMinutes() * 60000;
                taskWrapper.logger().info("Unable to acquire lock, will back off for " + lockBackOff / 60000 + "m");
                triggerTask(taskWrapper, vertx, lockBackOff);
              } else {
                final var errorBackOff = taskWrapper.task().configuration().errorBackOffInMinutes() * 60000;
                taskWrapper.logger().info("Error handling task, will back off for " + errorBackOff / 60000 + "m", throwable);
                triggerTask(taskWrapper, vertx, errorBackOff);
              }

            }
          );
      }
    );
    timers.put(taskWrapper.task().getClass(), timerId);
  }



  public record TaskWrapper(
    TimerTask task,
    Logger logger
  ) {
  }

}
