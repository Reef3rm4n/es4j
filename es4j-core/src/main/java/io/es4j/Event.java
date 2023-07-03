package io.es4j;


import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;


/**
 * The Event interface represents an event that is emitted from the Behaviour
 * interface as a result of processing commands. It must be implemented by all events.
 * Events are used to record state changes in an aggregate. Whenever the content
 * of the event changes, the schema version must be incremented.
 * Event extends Shareable and Serializable interfaces.
 */
public interface Event extends Shareable, Serializable {

  /**
   * Retrieves the schema version of this event. The schema version must be
   * incremented whenever the content of the event changes.
   * This method provides a default implementation and returns 0.
   *
   * @return The schema version as an integer, default is 0.
   */
  default int schemaVersion() {
    return 0;
  }

  /**
   * Retrieves the list of tags associated with this event.
   * Tags can be used for categorization or filtering of events.
   * This method provides a default implementation that returns an empty list.
   *
   * @return A list of tags as strings. Default is an empty list.
   */
  default List<String> tags() {
    return Collections.emptyList();
  }

  // TODO: Add mandatory event-type and use that instead of the event class when storing in event log
  // At startup, save the map of event-type -> class

}

