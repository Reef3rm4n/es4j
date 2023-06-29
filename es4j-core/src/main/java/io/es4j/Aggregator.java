package io.es4j;

import io.es4j.core.objects.Es4jError;
import io.vertx.core.json.JsonObject;
import io.es4j.core.exceptions.UnknownEvent;
import io.es4j.core.objects.ErrorSource;

public interface Aggregator<T extends Aggregate, E extends Event> {

  T apply(T aggregateState, E event);
  default int currentSchemaVersion() {
    return 0;
  }

  default E transformFrom(int schemaVersion, JsonObject event) {
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
