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

/**
 * The AsyncProjection interface defines the structure for event projections
 * that use a polling strategy.
 */
public interface AsyncProjection {

  /**
   * Apply the list of event objects to the projection.
   *
   * @param events the list of AggregateEvent objects to apply
   * @return a Uni representing the completion of the operation
   */
  Uni<Void> apply(List<AggregateEvent> events);

  /**
   * Optional filter to be applied to the event journal.
   *
   * @return an Optional containing the EventJournalFilter, if one is defined
   */
  default Optional<EventJournalFilter> filter() {
    return Optional.empty();
  }

  /**
   * Defines the polling policy for the projection.
   *
   * @return a Cron object representing the polling policy. Defaults to once every minute.
   */
  default Cron pollingPolicy() {
    return new CronParser(CronDefinitionBuilder
      .instanceDefinitionFor(CronType.UNIX)
    )
      .parse("*/1 * * * *");
  }

  /**
   * Setup the projection with the given Vertx and configuration.
   *
   * @param vertx the Vertx to use for the setup
   * @param configuration the configuration to use for the setup
   * @return a Uni representing the completion of the setup operation
   */
  Uni<Void> setup(Vertx vertx, JsonObject configuration);

  /**
   * Retrieve the class of the aggregate associated with the projection.
   *
   * @return the Class of the aggregate associated with the projection
   */
  Class<? extends Aggregate> aggregateClass();

}
