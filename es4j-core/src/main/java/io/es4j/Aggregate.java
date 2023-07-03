package io.es4j;

import io.es4j.core.objects.Es4jError;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Shareable;
import io.es4j.core.exceptions.UnknownEvent;
import io.es4j.core.objects.ErrorSource;

import java.io.Serializable;


/**
 * Aggregate root in es4j. It provides an identity, tenant information,
 * a schema version, and allows snapshot transformations.
 * Aggregate extends Shareable and Serializable interfaces.
 */
public interface Aggregate extends Shareable, Serializable {
  /**
   * Retrieves the unique identifier for this aggregate.
   *
   * @return The unique identifier as a string for this aggregate.
   */
  String aggregateId();
  /**
   * Retrieves the tenant information for this aggregate.
   * This method provides a default implementation and returns "default".
   *
   * @return The tenant as a string, default is "default".
   */
  default String tenant() {
    return "default";
  }
  /**
   * Retrieves the schema version of this aggregate.
   * This method provides a default implementation and returns 0.
   *
   * @return The schema version as an integer, default is 0.
   */
  default int schemaVersion() {
    return 0;
  }
  /**
   * Transforms the snapshot of the aggregate based on the specified schema version.
   * The default implementation of this method throws an UnknownEvent exception,
   * indicating that the schema version transformation is not supported.
   *
   * @param schemaVersion The target schema version to transform the snapshot to.
   * @param snapshot      The snapshot as a JsonObject that needs to be transformed.
   * @return The transformed aggregate, if the transformation is supported.
   * @throws UnknownEvent if the schema version transformation is not supported.
   */
  default Aggregate transformSnapshot(int schemaVersion, JsonObject snapshot) {
    throw new UnknownEvent(new Es4jError(
      ErrorSource.LOGIC,
      Aggregator.class.getName(),
      "missing schema versionTo " + schemaVersion,
      "could not transform event",
      "aggregate.event.transform",
      500
    )
    );
  }

}
