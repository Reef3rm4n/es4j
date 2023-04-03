package io.vertx.eventx.test.eventsourcing.behaviours;


import io.vertx.eventx.Behaviour;
import io.vertx.eventx.Event;
import io.vertx.eventx.test.eventsourcing.FakeAggregate;
import io.vertx.eventx.test.eventsourcing.commands.ChangeData;
import io.vertx.eventx.test.eventsourcing.events.DataChanged;

import java.util.List;

public class ChangeBehaviour implements Behaviour<FakeAggregate, ChangeData> {
  @Override
  public List<Event> process(final FakeAggregate state, final ChangeData command) {
    return List.of(new DataChanged(command.newData1()));
  }

}
