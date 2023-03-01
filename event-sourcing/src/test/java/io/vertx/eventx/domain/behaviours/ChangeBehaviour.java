package io.vertx.eventx.domain.behaviours;


import io.vertx.eventx.Behaviour;
import io.vertx.eventx.domain.FakeAggregate;
import io.vertx.eventx.domain.commands.ChangeData;
import io.vertx.eventx.domain.events.DataChanged;

import java.util.List;

public class ChangeBehaviour implements Behaviour<FakeAggregate, ChangeData> {
  @Override
  public List<Object> process(final FakeAggregate state, final ChangeData command) {
    return List.of(new DataChanged(command.newData1()));
  }

}
