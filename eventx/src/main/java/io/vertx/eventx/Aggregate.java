package io.vertx.eventx;

import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Shareable;
import io.vertx.eventx.exceptions.UnknownEvent;
import io.vertx.eventx.objects.ErrorSource;
import io.vertx.eventx.objects.EventxError;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public interface Aggregate extends Shareable, Serializable {

  String aggregateId();

  default String tenantID() {
    return "default";
  }

  default int schemaVersion() {
    return 0;
  }

  default Optional<Integer> snapshotEvery() {
    return Optional.empty();
  }

  default Aggregate transformSnapshot(int schemaVersion, JsonObject snapshot) {
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
