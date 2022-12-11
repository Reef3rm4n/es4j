package io.vertx.skeleton.task;

import io.vertx.skeleton.utils.CustomClassLoader;
import io.vertx.skeleton.models.OrmNotFoundException;
import io.vertx.skeleton.orm.RepositoryHandler;
import io.activej.inject.Injector;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.shareddata.Lock;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TaskDeployer {
  protected static final Logger LOGGER = LoggerFactory.getLogger(TaskDeployer.class);

  public static final Map<Class<?>, Long> timers = new HashMap<>();

  List<TaskWrapper> taskWrappers;
  private Vertx vertx;

  public void stopTimers() {
    timers.forEach((tClass, timerId) -> vertx.cancelTimer(timerId));
  }


  public void deploy(RepositoryHandler repositoryHandler, JsonObject configuration, final Injector injector) {
    this.vertx = repositoryHandler.vertx();
    if (CustomClassLoader.checkPresenceInBinding(injector, SynchronizedTask.class)) {
      this.taskWrappers = CustomClassLoader.loadFromInjector(injector, SynchronizedTask.class).stream()
        .map(task -> {
            LOGGER.info("Task found -> " + task.getClass().getName());
            final var taskConfiguration = configuration.getJsonObject(task.getClass().getSimpleName());
            return new TaskWrapper(task, taskConfiguration, LoggerFactory.getLogger(task.getClass()));
          }
        )
        .toList();
      taskWrappers.forEach(taskWrapper -> triggerTask(taskWrapper, repositoryHandler.vertx(), 10L));
    }
  }

  public static void triggerTask(TaskWrapper taskWrapper, Vertx vertx, Long throttle) {
    timers.remove(taskWrapper.task().getClass());
    final var timerId = vertx.setTimer(
      throttle,
      delay -> {
        final var start = Instant.now();
        final var lockUni = switch (taskWrapper.task().strategy()) {
          case CLUSTER_WIDE ->
            vertx.sharedData().getLock(taskWrapper.configuration().getString("lockName", taskWrapper.task().getClass().getName()));
          case LOCAL ->
            vertx.sharedData().getLocalLock(taskWrapper.configuration().getString("lockName", taskWrapper.task().getClass().getName()));
          case NONE -> Uni.createFrom().item(Lock.newInstance(() -> {
          }));
        };
        lockUni.flatMap(lock -> taskWrapper.task().performTask().onItemOrFailure().invoke((avoid, failure) -> lock.release()))
          .subscribe()
          .with(avoid -> {
              final var end = Instant.now();
              taskWrapper.logger().info("Task ran in " + Duration.between(start, end).toMillis() + "ms");
              final var emptyTaskBackOff = taskWrapper.configuration().getLong("emptyBackOff", 1L) * 60000;
              taskWrapper.logger().info("Task throttling for " + emptyTaskBackOff + "m");
              triggerTask(taskWrapper, vertx, emptyTaskBackOff);
            },
            throwable -> {
              if (throwable instanceof OrmNotFoundException) {
                final var emptyTaskBackOff = taskWrapper.configuration().getLong("emptyBackOff", 1L) * 60000;
                taskWrapper.logger().info("Task is empty, backing off for " + emptyTaskBackOff/1000 + "s");
                triggerTask(taskWrapper, vertx, emptyTaskBackOff);
              } else if (throwable instanceof NoStackTraceThrowable noStackTraceThrowable && noStackTraceThrowable.getMessage().contains("Timed out waiting to get lock")) {
                final var lockBackOff = taskWrapper.configuration().getLong("lockBackOff", 1L) * 60000;
                taskWrapper.logger().info("Unable to acquire lock !, task will back off for " + lockBackOff + "m");
                triggerTask(taskWrapper, vertx, lockBackOff);
              } else {
                final var errorBackOff = taskWrapper.configuration().getLong("errorBackOff", 1L) * 60000;
                taskWrapper.logger().info("Error handling batch !, task will back off for " + errorBackOff + "m");
                triggerTask(taskWrapper, vertx, errorBackOff);
              }
            }
          );
      }
    );
    timers.put(taskWrapper.task().getClass(), timerId);
  }

  public record TaskWrapper(
    SynchronizedTask task,
    JsonObject configuration,
    Logger logger
  ) {
  }

}
