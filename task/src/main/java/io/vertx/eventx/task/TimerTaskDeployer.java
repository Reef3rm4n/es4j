package io.vertx.eventx.task;

import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.eventx.common.CustomClassLoader;
import io.activej.inject.Injector;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.shareddata.Lock;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TimerTaskDeployer {

  protected static final Logger LOGGER = LoggerFactory.getLogger(TimerTaskDeployer.class);

  public static final Map<Class<?>, Long> timers = new HashMap<>();

  List<TaskWrapper> taskWrappers;
  private Vertx vertx;

  public void close() {
    timers.forEach((tClass, timerId) -> vertx.cancelTimer(timerId));
  }
  public void deploy(final Injector injector) {
    this.vertx = injector.getInstance(Vertx.class);
    if (CustomClassLoader.checkPresenceInBinding(injector, TimerTask.class)) {
      this.taskWrappers = CustomClassLoader.loadFromInjector(injector, TimerTask.class).stream()
        .map(task -> {
            LOGGER.info("Task found -> " + task.getClass().getName());
            return new TaskWrapper(task, LoggerFactory.getLogger(task.getClass()));
          }
        )
        .toList();
      taskWrappers.forEach(taskWrapper -> triggerTask(taskWrapper, vertx, 10L));
    }
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
