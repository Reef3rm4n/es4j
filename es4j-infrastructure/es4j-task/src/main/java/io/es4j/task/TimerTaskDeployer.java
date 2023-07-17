package io.es4j.task;

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
    timers.clear();
  }

  public void deploy(TimerTask timerTask) {
    final var wrapper = new TaskWrapper(timerTask, LoggerFactory.getLogger(timerTask.getClass()));
    triggerTask(wrapper, vertx, Duration.ofMillis(100));
  }

  public static void triggerTask(TaskWrapper taskWrapper, Vertx vertx, Duration throttle) {
    timers.remove(taskWrapper.task().getClass());
    taskWrapper.logger().info("Starting task");
    final var timerId = vertx.setTimer(
      throttle.toMillis(),
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
              final var emptyTaskBackOff = taskWrapper.task().configuration().throttle();
              taskWrapper.logger().info("Task ran in " + Duration.between(start, end).toMillis() + "ms. Next execution in " + emptyTaskBackOff.getSeconds() + "s");
              triggerTask(taskWrapper, vertx, emptyTaskBackOff);
            },
            throwable -> {
              final var end = Instant.now();
              if (taskWrapper.task.configuration().knownInterruptions().stream().anyMatch(t -> t.isAssignableFrom(throwable.getClass()))) {
                taskWrapper.logger().debug("Task interrupted by" + throwable.getClass().getSimpleName() + " after " + Duration.between(start, end).toMillis() + "ms");
                taskWrapper.logger().info("Interrupted, backing off for {}", taskWrapper.task().configuration().interruptionBackOff());
                triggerTask(taskWrapper, vertx, taskWrapper.task().configuration().interruptionBackOff());
              } else if (throwable instanceof NoStackTraceThrowable noStackTraceThrowable && noStackTraceThrowable.getMessage().contains("Timed out waiting to get lock")) {
                taskWrapper.logger().info("Unable to acquire lock, will back off for {}", taskWrapper.task().configuration().lockBackOff());
                triggerTask(taskWrapper, vertx, taskWrapper.task().configuration().lockBackOff());
              } else {
                taskWrapper.logger().info("Error handling task, will back off for {}", taskWrapper.task().configuration().errorBackOff(), throwable);
                triggerTask(taskWrapper, vertx, taskWrapper.task().configuration().errorBackOff());
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
