package io.vertx.eventx.core;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import io.smallrye.mutiny.Uni;
import io.vertx.eventx.task.CronTask;
import io.vertx.eventx.task.CronTaskConfiguration;
import io.vertx.eventx.task.CronTaskConfigurationBuilder;

public class EventlogCompression implements CronTask {
  @Override
  public Uni<Void> performTask() {
    return null;
  }

  @Override
  public CronTaskConfiguration configuration() {
    return CronTaskConfigurationBuilder.builder()
      .cron(new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)).parse("0 0 * * *"))
      .build();
  }
}
