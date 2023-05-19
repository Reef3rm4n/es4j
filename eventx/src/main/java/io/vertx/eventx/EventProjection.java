package io.vertx.eventx;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import io.smallrye.mutiny.Uni;
import io.vertx.eventx.core.objects.EventJournalFilter;
import io.vertx.eventx.core.objects.PolledEvent;

import java.util.List;
import java.util.Optional;

public interface EventProjection {

  Uni<Void> apply(List<PolledEvent> events);

  default Optional<EventJournalFilter> filter() {
    return Optional.empty();
  }

  default String tenantID() {
    return "default";
  }
  //

  default Cron pollingPolicy() {
    return new CronParser(CronDefinitionBuilder
      .instanceDefinitionFor(CronType.UNIX)
    )
      .parse("*/1 * * * *");
  }


}
