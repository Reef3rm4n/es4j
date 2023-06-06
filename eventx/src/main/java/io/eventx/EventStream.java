package io.eventx;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import io.eventx.core.objects.EventJournalFilter;
import io.eventx.core.objects.PolledEvent;
import io.smallrye.mutiny.Uni;

import java.util.List;
import java.util.Optional;

public interface EventStream {

  Uni<Void> apply(PolledEvent event);

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
