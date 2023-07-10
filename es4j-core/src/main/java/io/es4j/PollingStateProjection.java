package io.es4j;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import io.es4j.core.objects.AggregateState;
import io.smallrye.mutiny.Uni;

/**
 * An interface for polling state projections of aggregates in an event sourcing system.
 *
 * <p>This interface defines methods for updating the current state of an aggregate, and provides
 * default implementations for retrieving the tenant and the polling policy.</p>
 *
 * <p>Implementors of this interface should provide a specific implementation of the
 * {@link #update(AggregateState)} method, which is used to update the current state
 * of an aggregate.</p>
 *
 * @param <T> the type of the aggregate
 */
public interface PollingStateProjection<T extends Aggregate> {

  /**
   * Updates the current state of an aggregate.
   *
   * @param currentState the current state of the aggregate
   * @return a Uni<Void> which represents the completion of the update operation
   */
  Uni<Void> update(AggregateState<T> currentState);

  /**
   * Returns the tenant for this polling state projection.
   *
   * <p>The default implementation returns "default", but this can be overridden by subclasses.</p>
   *
   * @return a string representing the tenant
   */
  default String tenant() {
    return "default";
  }

  /**
   * Returns the polling policy for this polling state projection.
   *
   * <p>The default implementation returns a Cron policy that triggers every minute. This can be
   * overridden by subclasses to provide a different polling policy.</p>
   *
   * @return a Cron object representing the polling policy
   */
  default Cron pollingPolicy() {
    return new CronParser(CronDefinitionBuilder
      .instanceDefinitionFor(CronType.UNIX)
    )
      .parse("*/1 * * * *");
  }
}

