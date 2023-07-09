package io.es4j;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.es4j.core.objects.CommandOptions;
import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
/**
 * The Command interface represents a command that can be issued to an aggregate.
 * Commands are used to perform actions or change the state of an aggregate.
 * All commands that are issued to an aggregate should implement this interface.
 * Command extends Shareable and Serializable interfaces.
 */
public interface Command extends Shareable, Serializable {

  /**
   * Retrieves the aggregate identifier to which this command is targeted.
   *
   * @return The aggregate identifier as a string.
   */
  String aggregateId();

  /**
   * Retrieves the tenant information for this command.
   * This method provides a default implementation and returns "default".
   *
   * @return The tenant as a string, default is "default".
   */
  default String tenant() {
    return "default";
  }

  /**
   * Generates and retrieves a unique identifier for this command.
   * This method provides a default implementation that generates a random UUID.
   *
   * @return A unique identifier as a string.
   */
  default String uniqueId() {
    return UUID.randomUUID().toString();
  }

  /**
   * Retrieves the options associated with this command.
   * This method provides a default implementation that returns the default options.
   *
   * @return The options associated with this command, as a CommandOptions object.
   */
  default CommandOptions options() {
    return CommandOptions.defaultOptions();
  }

}
