package io.es4j;

import java.util.Collections;
import java.util.List;

/**
 * The Bootstrap interface is responsible for providing the framework
 * with the necessary information regarding the aggregate roots that
 * need to be taken into consideration.
 */
public interface Bootstrap {

  /**
   * Provides the class of the aggregate root that should be taken into
   * consideration by the framework.
   *
   * @return The class of the aggregate that extends the Aggregate interface.
   */
  Class<? extends Aggregate> aggregateClass();

  /**
   * Provides file configurations that may be used for additional setup.
   * This method provides a default implementation that returns an empty list.
   *
   * @return A list of file configurations as strings. Default is an empty list.
   */
  default List<String> fileConfigurations() {
    return Collections.emptyList();
  }

}
