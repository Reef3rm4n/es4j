package io.es4j.task;

import com.cronutils.model.time.ExecutionTime;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;


public class CronTaskDeployer {

  public static final Map<Class<?>, Long> timers = new HashMap<>();
  private final Vertx vertx;

  public CronTaskDeployer(
    Vertx vertx
  ) {
    this.vertx = vertx;
  }

  public void close() {
    timers.forEach((tClass, timerId) -> vertx.cancelTimer(timerId));
    timers.clear();
  }

  public void deploy(CronTask timerTask) {
    final var wrapper = new TaskWrapper(timerTask, LoggerFactory.getLogger(timerTask.getClass()));
    final var execution = nextExecution(wrapper);
    triggerTask(wrapper, vertx, execution);
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
              taskWrapper.logger().info("cron-task ran in {}", Duration.between(start, Instant.now()).toMillis());
              triggerTask(taskWrapper, vertx, nextExecution(taskWrapper));
            },
            throwable -> {
              taskWrapper.logger().info("cron-task ran in {}", Duration.between(start, Instant.now()).toMillis());
              if (taskWrapper.task.configuration().knownInterruptions().stream().anyMatch(t -> t.isAssignableFrom(throwable.getClass()))) {
                taskWrapper.logger().debug("Interrupted by {} ", throwable.getClass().getSimpleName());
              } else if (throwable instanceof NoStackTraceThrowable noStackTraceThrowable && noStackTraceThrowable.getMessage().contains("Timed out waiting to get lock")) {
                taskWrapper.logger().debug("Unable to acquire lock");
              } else {
                taskWrapper.logger().error("Error handling cron-task", throwable);
              }
              triggerTask(taskWrapper, vertx, nextExecution(taskWrapper));
            }
          );
      }
    );
    timers.put(taskWrapper.task().getClass(), timerId);
  }

  public Duration nextExecution(TaskWrapper task) {
    final var executionTime = ExecutionTime.forCron(task.task().configuration().cron());
    final var duration = executionTime.timeToNextExecution(ZonedDateTime.now()).orElseThrow();
    task.logger().info("CronTask next execution {}", duration);
    return duration;
  }

  public record TaskWrapper(
    CronTask task,
    Logger logger
  ) {
  }

}
