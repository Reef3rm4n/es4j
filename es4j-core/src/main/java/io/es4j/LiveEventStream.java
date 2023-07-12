package io.es4j;

import io.es4j.infrastructure.models.Event;
import io.smallrye.mutiny.Uni;

/**
 * The LiveEventStream interface defines the structure for live streams of eventTypes
 * within the framework. Implementors of this interface will be called in a best-effort
 * manner for emitted eventTypes.
 */
public interface LiveEventStream {

  /**
   * Apply an Event to the live stream. This method will be called in a best-effort
   * manner for emitted eventTypes.
   *
   * @param event the Event to apply
   * @return a Uni representing the completion of the operation
   */
  Uni<Void> apply(Event event);

  /**
   * Retrieve the tenant associated with the live event stream.
   *
   * @return a String representing the tenant. Defaults to "default".
   */
  default String tenant() {
    return "default";
  }
}
