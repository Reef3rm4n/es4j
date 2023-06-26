package io.eventx;

import io.eventx.core.objects.EventxError;
import io.vertx.core.json.JsonObject;
import io.eventx.core.exceptions.UnknownEvent;
import io.eventx.core.objects.ErrorSource;

public interface Aggregator<T extends Aggregate, E extends Event> {

  T apply(T aggregateState, E event);
  default int currentSchemaVersion() {
    return 0;
  }

  default E transformFrom(int schemaVersion, JsonObject event) {
    throw new UnknownEvent(new EventxError(
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
