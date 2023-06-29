package io.es4j;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import io.es4j.core.objects.EventJournalFilter;
import io.smallrye.mutiny.Uni;
import io.es4j.core.objects.AggregateEvent;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.List;
import java.util.Optional;

public interface PollingEventProjection {

  Uni<Void> apply(List<AggregateEvent> events);

  default Optional<EventJournalFilter> filter() {
    return Optional.empty();
  }

  default String tenant() {
    return "default";
  }
  //

  default Cron pollingPolicy() {
    return new CronParser(CronDefinitionBuilder
      .instanceDefinitionFor(CronType.UNIX)
    )
      .parse("*/1 * * * *");
  }

  Uni<Void> setup(Vertx vertx, JsonObject configuration);

  Class<? extends Aggregate> aggregateClass();


}
