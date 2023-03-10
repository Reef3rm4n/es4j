package io.vertx.eventx.test.eventsourcing.domain.behaviours;


import io.vertx.eventx.Behaviour;
import io.vertx.eventx.Event;
import io.vertx.eventx.test.eventsourcing.domain.FakeAggregate;
import io.vertx.eventx.test.eventsourcing.domain.commands.ChangeData;
import io.vertx.eventx.test.eventsourcing.domain.events.DataChanged;

import java.util.List;

public class ChangeBehaviour implements Behaviour<FakeAggregate, ChangeData> {
  @Override
  public List<Event> process(final FakeAggregate state, final ChangeData command) {
    return List.of(new DataChanged(command.newData1()));
  }

}
