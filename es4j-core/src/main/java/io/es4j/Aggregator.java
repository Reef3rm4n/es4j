package io.es4j;

import io.es4j.core.objects.Es4jError;
import io.vertx.core.json.JsonObject;
import io.es4j.core.exceptions.UnknownEvent;
import io.es4j.core.objects.ErrorSource;

/**
 * The Aggregator interface represents an entity responsible for
 * applying eventTypes to an aggregate root and handling event version migration
 * in an event sourcing based system.
 *
 * @param <T> The type of the aggregate, which must implement the Aggregate interface.
 * @param <E> The type of the event, which must implement the Event interface.
 */
public interface Aggregator<T extends Aggregate, E extends Event> {

  /**
   * Applies the given event to the provided aggregate state and returns the
   * updated aggregate.
   *
   * @param aggregateState The current state of the aggregate.
   * @param event The event to be applied to the aggregate.
   * @return The updated state of the aggregate after the event has been applied.
   */
  T apply(T aggregateState, E event);

  /**
   * Retrieves the current schema version that the aggregator is capable of processing.
   * This method provides a default implementation and returns 0.
   *
   * @return The current schema version as an integer, default is 0.
   */
  default int schemaVersion() {
    return 0;
  }

  /**
   * Transforms the event based on the specified schema version.
   * The default implementation of this method throws an UnknownEvent exception,
   * indicating that the event version transformation is not supported.
   *
   * @param schemaVersion The target schema version to transform the event to.
   * @param event The event as a JsonObject that needs to be transformed.
   * @return The transformed event, if the transformation is supported.
   * @throws UnknownEvent if the event version transformation is not supported.
   */
  default E transformFrom(int schemaVersion, JsonObject event) {
    throw new UnknownEvent(new Es4jError(
      ErrorSource.LOGIC,
      Aggregator.class.getName(),
      "missing schema versionTo " + schemaVersion,
      "could not transform event %s to schema version %d".formatted(event, schemaVersion),
      "aggregate.event.transform",
      500
    )
    );
  }

  String eventType();

}

