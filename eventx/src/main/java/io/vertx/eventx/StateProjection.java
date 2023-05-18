package io.vertx.eventx;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import io.smallrye.mutiny.Uni;
import io.vertx.eventx.objects.AggregateState;

import java.time.Duration;

public interface StateProjection<T extends Aggregate> {

  Uni<Void> update(AggregateState<T> currentState);
  default String tenantID() {
    return "default";
  }

  default Cron pollingPolicy() {
    return new CronParser(CronDefinitionBuilder
      .instanceDefinitionFor(CronType.UNIX)
    )
      .parse("*/1 * * * *");
  }

}
