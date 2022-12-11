package io.vertx.skeleton.evs;

import java.util.List;

public class FakeCommandBehaviour implements CommandBehaviour<FakeEntityAggregate, FakeCommand> {
  @Override
  public List<Object> process(final FakeEntityAggregate state, final FakeCommand command) {
    return null;
  }

}
