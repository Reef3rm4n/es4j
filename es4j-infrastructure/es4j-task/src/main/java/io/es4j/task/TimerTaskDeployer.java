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

  private final Map<Class<?>, Long> timers;
  private final Vertx vertx;

  public TimerTaskDeployer(
    Vertx vertx
  ) {
    this.vertx = vertx;
    this.timers = new HashMap<>();
  }

  public void close() {
    timers.forEach((tClass, timerId) -> vertx.cancelTimer(timerId));
    timers.clear();
  }

  public void deploy(TimerTask timerTask) {
    final var wrapper = new TaskWrapper(timerTask, LoggerFactory.getLogger(timerTask.getClass()));
    triggerTask(wrapper, vertx, Duration.ofMillis(100));
  }

  public void triggerTask(TaskWrapper taskWrapper, Vertx vertx, Duration throttle) {
    timers.remove(taskWrapper.task().getClass());
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
//              taskWrapper.logger().debug("Task ran in " + Duration.between(start, end).toMillis() + "ms. Next execution in " + emptyTaskBackOff.getSeconds() + "s");
              triggerTask(taskWrapper, vertx, emptyTaskBackOff);
            },
            throwable -> {
              final var end = Instant.now();
              if (taskWrapper.task.configuration().finalInterruption().isPresent() && taskWrapper.task.configuration().finalInterruption().get().isAssignableFrom(throwable.getClass())) {
                taskWrapper.logger().info("Final interruption {} reached, task wont be rescheduled", throwable.getClass().getSimpleName());
              } else if (throwable instanceof NoStackTraceThrowable noStackTraceThrowable && noStackTraceThrowable.getMessage().contains("Timed out waiting to get lock")) {
//                taskWrapper.logger().debug("Unable to acquire lock after {}ms, will back off for {}", Duration.between(start, end).toMillis(), taskWrapper.task().configuration().lockBackOff());
                triggerTask(taskWrapper, vertx, taskWrapper.task().configuration().lockBackOff());
              } else {
//                taskWrapper.logger().debug("Error handling task after {}ms, will back off for {}", Duration.between(start, end).toMillis(), taskWrapper.task().configuration().errorBackOff(), throwable);
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
