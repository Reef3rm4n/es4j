package io.es4j;

import io.es4j.core.objects.AggregateConfiguration;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * The Bootstrap interface is responsible for providing the framework
 * with the necessary information regarding the aggregate roots that
 * need to be taken into consideration.
 */
public interface Es4jDeployment {

  /**
   * Provides the class of the aggregate root that should be taken into
   * consideration by the framework.
   *
   * @return The class of the aggregate that extends the Aggregate interface.
   */
  Class<? extends Aggregate> aggregateClass();

  default AggregateConfiguration aggregateConfiguration() {
    return new AggregateConfiguration(
      Duration.ofHours(1),
      100,
      100
    );
  }

  /**
   * Provides file configurations that may be used for additional setup.
   * This method provides a default implementation that returns an empty list.
   *
   * @return A list of file configurations as strings. Default is an empty list.
   */
  default List<String> fileBusinessRules() {
    return Collections.emptyList();
  }

  default List<String> tenants() {
    return List.of("default");
  }

  default String infrastructureConfiguration() {
    return "infrastructure";
  }


}
