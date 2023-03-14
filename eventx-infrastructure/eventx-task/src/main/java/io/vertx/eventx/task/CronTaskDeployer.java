package io.vertx.eventx.task;

import io.activej.inject.Injector;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.launcher.EventxMain;
import io.vertx.eventx.queue.MessageProducer;
import io.vertx.eventx.queue.postgres.PgMessageProducer;
import io.vertx.mutiny.core.Vertx;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.eventx.common.CustomClassLoader;
import java.util.List;


public class CronTaskDeployer {

  protected static final Logger LOGGER = LoggerFactory.getLogger(CronTaskDeployer.class);
  private Vertx vertx;


  public Uni<Void> deploy(final Injector injector) {
    if (CustomClassLoader.checkPresenceInBinding(injector, CronTask.class)) {
      EventxMain.MAIN_MODULES.add(new AbstractModule() {
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
          return new CronTaskProcessor(tasks,producer);
        }
        @Inject
        @Provides
        MessageProducer producer(RepositoryHandler repositoryHandler) {
          return new PgMessageProducer(repositoryHandler);
        }
      }
      );
    }

    return Uni.createFrom().voidItem();
  }


}
