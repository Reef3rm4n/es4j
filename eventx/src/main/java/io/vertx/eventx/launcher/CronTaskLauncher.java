package io.vertx.eventx.launcher;


import io.activej.inject.Injector;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.eventx.infrastructure.misc.CustomClassLoader;
import io.vertx.eventx.queue.MessageProducer;
import io.vertx.eventx.queue.postgres.PgMessageProducer;
import io.vertx.eventx.queue.postgres.models.MessageRecordQuery;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.eventx.sql.models.QueryOptions;
import io.vertx.eventx.task.CronTask;
import io.vertx.eventx.task.CronTaskMessage;
import io.vertx.eventx.task.CronTaskProcessor;

import java.util.List;

public class CronTaskLauncher {

  protected static final Logger LOGGER = LoggerFactory.getLogger(CronTaskLauncher.class);
  private CronTaskLauncher() {
  }

  public static void deploy(final Injector injector) {
    if (CustomClassLoader.checkPresenceInBinding(injector, CronTask.class)) {
      bootstrap();
      kickstart(injector)
        .subscribe()
        .with(avoid -> LOGGER.info("Cron tasks kickstarted")
          , throwable -> LOGGER.error("Unable to kickstart cron tasks", throwable)
        );
    }
  }
  private static void bootstrap() {
    EventxMain.MAIN_MODULES.add(
      new AbstractModule() {
        @Inject
        @Provides
        List<CronTask> cronTasks(Injector injector) {
          return CustomClassLoader.loadFromInjector(injector, CronTask.class);
        }
        @Inject
        @Provides
        CronTaskProcessor cronTaskProcessor(
          List<CronTask> tasks, MessageProducer producer
        ) {
          return new CronTaskProcessor(tasks, producer);
        }

        @Inject
        @Provides
        MessageProducer producer(RepositoryHandler repositoryHandler) {
          return new PgMessageProducer(repositoryHandler);
        }
      }
    );
  }

  private static Uni<Void> kickstart(Injector injector) {
    final var producer = new PgMessageProducer(injector.getInstance(RepositoryHandler.class));
    final var tasks = CustomClassLoader.loadFromInjector(injector, CronTask.class);
    final var existingTaskNames = tasks.stream().map(t -> t.getClass().getName()).toList();
    return producer.query(query(tasks), CronTaskMessage.class)
      .flatMap(messages -> {
          final var presentTasks = messages.stream()
            .map(cronTaskMessageMessage -> {
                final var split = cronTaskMessageMessage.messageId().split("::");
                return split[0];
              }
            ).toList();
          LOGGER.debug("Tasks already scheduled -> " + presentTasks);
          final var tasksToKickstart = existingTaskNames.stream()
            .filter(messageTaskName -> presentTasks.stream().anyMatch(messageTaskName::equals))
            .map(messageTaskName -> tasks.stream().filter(t -> t.getClass().getName().equals(messageTaskName)).findFirst().orElseThrow())
            .map(CronTaskProcessor::taskScheduledMessage)
            .toList();
          LOGGER.debug("Tasks that need kickstart -> " + tasksToKickstart.stream().map(Object::getClass).map(Class::getName).toList());
          return producer.enqueue(tasksToKickstart);
        }
      );
  }

  private static MessageRecordQuery query(List<CronTask> tasks) {
    final var ids = tasks.stream()
      .map(task -> task.getClass().getName() + "::" + "*")
      .toList();
    return new MessageRecordQuery(
      ids,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      new QueryOptions(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        "default"
      )
    );
  }


}
