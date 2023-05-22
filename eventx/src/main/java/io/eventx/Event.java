package io.eventx;


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

}
