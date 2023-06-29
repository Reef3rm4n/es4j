package io.es4j;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import io.es4j.core.objects.AggregateState;
import io.smallrye.mutiny.Uni;

public interface PollingStateProjection<T extends Aggregate> {

  Uni<Void> update(AggregateState<T> currentState);
  default String tenant() {
    return "default";
  }

  default Cron pollingPolicy() {
    return new CronParser(CronDefinitionBuilder
      .instanceDefinitionFor(CronType.UNIX)
    )
      .parse("*/1 * * * *");
  }

}
