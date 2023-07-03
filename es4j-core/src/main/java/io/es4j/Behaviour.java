package io.es4j;

import java.util.List;

/**
 * The Behaviour interface represents the behavior of an aggregate in response
 * to a command. It is responsible for processing commands, validating them, and
 * emitting events that should be applied to the aggregate.
 *
 * @param <T> The type of the aggregate, which must implement the Aggregate interface.
 * @param <C> The type of the command, which must implement the Command interface.
 */
public interface Behaviour<T extends Aggregate, C extends Command> {

  /**
   * Processes the given command against the provided aggregate state.
   * This involves validating the command and emitting events that represent
   * the changes to be applied to the aggregate.
   *
   * @param state   The current state of the aggregate.
   * @param command The command to be processed.
   * @return A list of events that should be applied to the aggregate.
   */
  List<Event> process(T state, C command);

}
