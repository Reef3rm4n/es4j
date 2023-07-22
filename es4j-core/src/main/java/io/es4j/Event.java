package io.es4j;


import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;


/**
 * The Event interface represents an event that is emitted from the Behaviour
 * interface as a result of processing commands. It must be implemented by all eventTypes.
 * Events are used to record state changes in an aggregate. Whenever the content
 * of the event changes, the schema version must be incremented.
 * Event extends Shareable and Serializable interfaces.
 */
public interface Event extends Shareable, Serializable {

  /**
   * Retrieves the list of tags associated with this event.
   * Tags can be used for categorization or filtering of eventTypes.
   * This method provides a default implementation that returns an empty list.
   *
   * @return A list of tags as strings. Default is an empty list.
   */
  default List<String> tags() {
    return Collections.emptyList();
  }

}

