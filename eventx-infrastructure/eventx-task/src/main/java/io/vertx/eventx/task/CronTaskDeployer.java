package io.vertx.eventx.task;

import com.cronutils.model.time.ExecutionTime;
import io.activej.inject.Injector;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.shareddata.Lock;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.eventx.sql.exceptions.NotFound;
import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.QueryOptions;
import io.vertx.eventx.common.CustomClassLoader;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;


public class CronTaskDeployer {

  protected static final Logger LOGGER = LoggerFactory.getLogger(CronTaskDeployer.class);

  private Long timer;
  private Vertx vertx;
  private Repository<CronTaskKey, CronTaskRecord, CronTaskQuery> repository;
  private List<CronTaskWrapper> tasks;

  public void stopTimer() {
    if (timer != null)
      vertx.cancelTimer(timer);
  }

  public Uni<Void> deploy(RepositoryHandler repositoryHandler, final Injector injector) {
    if (CustomClassLoader.checkPresenceInBinding(injector, CronTask.class)) {
      this.vertx = repositoryHandler.vertx();
      this.repository = new Repository<>(CronTaskMapper.INSTANCE, repositoryHandler);
      this.tasks = CustomClassLoader.loadFromInjector(injector, CronTask.class).stream()
        .map(task -> {
            LOGGER.info("Task found -> " + task.getClass().getName());
            return new CronTaskWrapper(task, LoggerFactory.getLogger(task.getClass()));
          }
        )
        .toList();
      startTimerStream();
      return registerTasks();
    }
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> registerTasks() {
    return Multi.createFrom().iterable(tasks)
      .onItem().transformToUniAndMerge(task -> findOrCreateCronRecord(task.task))
      .collect().asList()
      .replaceWithVoid();
  }

  public void startTimerStream() {
    timer = vertx.setTimer(
      60000,
      delay -> {
        final var start = Instant.now();
        performTasks()
          .subscribe()
          .with(
            avoid -> LOGGER.info("Tasks performed in " + Duration.between(start, Instant.now()).toMillis() + " ms"),
            throwable -> LOGGER.error("Tasks interrupted after " + Duration.between(start, Instant.now()).toMillis() + "ms", throwable)
          );
      }
    );
  }

  public Uni<Void> performTasks() {
    return repository.stream(
      cronTaskRecord -> {
        final var cronTask = tasks.stream().filter(task -> task.getClass().getName().equals(cronTaskRecord.taskClass()))
          .findFirst().orElseThrow();
        final var lockProvider = switch (cronTask.task().configuration().lockLevel()) {
          case CLUSTER_WIDE -> vertx.sharedData().getLock(cronTask.task().getClass().getName());
          case LOCAL -> vertx.sharedData().getLocalLock(cronTask.task().getClass().getName());
          case NONE -> Uni.createFrom().item(Lock.newInstance(() -> {
              }
            )
          );
        };
        final var start = Instant.now();
        lockProvider
          .flatMap(
            aLock -> cronTask.task().performTask()
              .onItemOrFailure().invoke((item, failure) -> aLock.release())
              .onFailure().recoverWithNull()
          )
          .flatMap(avoid -> repository.updateByKey(cronTaskRecord.newExecutionTime(calculateNextExecutionTime(cronTask.task))))
          .subscribe()
          .with(
            avoid -> cronTask.logger().info("Task performed in " + Duration.between(start, Instant.now()).toMillis() + " ms"),
            throwable -> cronTask.logger().error("Task interrupted after " + Duration.between(start, Instant.now()).toMillis() + "ms", throwable)
          );
      }
      , new CronTaskQuery(
        tasks.stream().map(t -> t.getClass().getSimpleName()).toList(),
        Instant.now(),
        null,
        null,
        null,
        QueryOptions.simple()
      )
    )
      .onFailure(NotFound.class).recoverWithNull();
  }


  public Uni<CronTaskRecord> findOrCreateCronRecord(CronTask task) {
    return repository.selectByKey(new CronTaskKey(task.getClass().getSimpleName()))
      .onFailure(NotFound.class)
      .recoverWithUni(() -> repository.insert(new CronTaskRecord(
            task.getClass().getSimpleName(),
            null,
            calculateNextExecutionTime(task),
            BaseRecord.newRecord()
          )
        )
      );
  }


  public Instant calculateNextExecutionTime(CronTask task) {
    final var executionTime = ExecutionTime.forCron(task.configuration().cron());
    return executionTime.nextExecution(ZonedDateTime.now()).orElseThrow().toInstant();
  }


  public record CronTaskWrapper(
    CronTask task,
    Logger logger
  ) {
  }

}
