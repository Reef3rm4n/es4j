package io.es4j;


import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;


public interface Event extends Shareable, Serializable {

  default int schemaVersion() {
    return 0;
  }

  default List<String> tags() {
    return Collections.emptyList();
  }

  // todo add mandatory event-type and use that instead of the event class when storing in event log
  // at startup save the map of event-type -> class

}
